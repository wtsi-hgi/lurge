import base64
from itertools import repeat
from lurge_types.group_report import GroupReport
import os
import datetime
import subprocess
import logging
import multiprocessing
import gzip
import typing as T

import ldap

import db.common
import db.report
import utils.finder
import utils.ldap
import utils.tsv

import db_config as config

from directory_config import PSEUDO_GROUPS, WRSTAT_DIR, REPORT_DIR, VOLUMES, GROUP_DIRECTORIES, LOGGING_CONFIG


def scanDirectory(directory: str) -> T.Optional[str]:
    """
    process_wrstat helper function. Scans 'directory' for .imirrored, and returns
    the directory if it was found and None otherwise.

    :param directory: Directory to scan
    """
    with os.scandir(directory) as items:
        for item in items:
            if item.name == ".imirrored":
                return directory

    return None


def get_group_data_from_wrstat(wr_file: str, ldap_pis: T.Dict[str, str], ldap_groups: T.Dict[str, str], logger: logging.Logger) -> T.Tuple[str, T.List[GroupReport]]:
    """
    Processes a single wrstat output file and creates a list of GroupReports.
    Intended to be ran multiple times concurrently for multiple files.

    :param wr_file: File name of wrstat output file to process
    :param ldap_pis: Group ids to PI surname dictionary
    :param ldap_groups: Group ids to group name dictionary
    :param logger: logging.Logger object to log to

    :returns: volume (str), group_data (List[GroupReport])
    Example:
        "scratch123", [
            GroupReport{
                group_name: "Group Name",
                pi_name: "PI",
                usage: 12345,
                ...
            }
        , ...] 
    """

    volume = wr_file.split('/')[-1].split('.')[0].split('_')[1] # wrstat
    # volume = "scratch" + wr_file.split('/')[-1].split('.')[0].split('_')[1] # mpistat

    groups: T.Dict[str, GroupReport] = {}
    for gid, group_name in ldap_groups.items():
        groups[str(gid)] = GroupReport(
            str(gid), group_name, ldap_pis[gid], volume)
    for gid, group_name, pi in PSEUDO_GROUPS.values():
        groups[str(gid)] = GroupReport(str(gid), group_name, pi, volume)

    lines_processed = 0
    logger.info("Opening {} for reading...".format(wr_file))

    with gzip.open(wr_file, 'rt') as wr_text:
        # each line in the wrstat file has the following whitespace separated
        # fields:
        # base64 encoded path, file size, owner uid, owner gid, last access time,
        # last modification time, last status change time, object type (file,
        # directory or link), inode number, number of links, device id
        for line in wr_text:

            # print out progress report every ~30 seconds
            if lines_processed % 5000000 == 0:
                logger.debug(
                    f"{lines_processed} records processed for {volume}")

            line = line.split()

            gid = line[3]
            file_path = base64.b64decode(line[0]).decode("UTF-8", "replace")
            for psuedo_group_path in PSEUDO_GROUPS.keys():
                if file_path.startswith(psuedo_group_path):
                    gid = str(PSEUDO_GROUPS[psuedo_group_path][0])

            try:
                try:
                    groups[gid].usage += int(
                        int(line[1]) / int(line[9]))
                except ZeroDivisionError:
                    # This should almost never happen, but it did! Looks like a
                    # file can get 'stat'ed in the middle of being deleted,
                    # which makes it show a hard link count of 0.
                    pass

                try:
                    # only update the group's last edit time if it's more recent
                    if (int(line[5]) > groups[gid].last_modified):
                        # make sure the timestamp isn't in the future
                        now = datetime.datetime.now()
                        now_unix = int(datetime.datetime.timestamp(now))
                        if(now_unix > int(line[5])):
                            groups[gid].last_modified = int(line[5])
                except ValueError:
                    continue
            except KeyError:
                # the first time the group appears, add it to the dictionary
                # its name will be found later
                try:
                    groups[gid] = GroupReport.create_non_humgen(
                        str(gid), volume, int(line[1]), int(line[5]))
                except IndexError:
                    continue

            lines_processed += 1

    # gets the Unix timestamp of when the wrstat file was created
    # int() truncates away the sub-second measurements
    wrstat_date_unix = int(os.stat(wr_file).st_mtime)
    group_data: T.List[GroupReport] = []
    for gid, group in groups.items():
        # updates the group names for groups discovered during wrstat crawl
        if group.group_name is None:
            # connection times out too quickly to be declared elsewhere
            ldap_con = utils.ldap.getLDAPConnection()
            try:
                result = ldap_con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
                                           ldap.SCOPE_ONELEVEL, "(gidNumber={})".format(gid), ["cn"])
            except ValueError:
                continue
            try:
                group.group_name = result[0][1]['cn'][0].decode('UTF-8')
            except IndexError:
                # nothing found in LDAP for this group id, skip the rest of
                # this loop
                continue

        # let it calculate its last modified time relative to the wrstat file time
        group.calculate_last_modified_rel(wrstat_date_unix)

        # lfs quota query is split into a list based on whitespace, and the
        # fourth element is taken as the quota. it's in kibibytes though, so it
        # needs to be multiplied by 1024
        try:
            group.quota = int(subprocess.check_output(["lfs", "quota", "-gq", group.group_name,
                                                       "/lustre/{}".format(volume)], encoding="UTF-8").split()[3]) * 1024
        except subprocess.CalledProcessError:
            # some groups don't have mercury as a member, which means their
            # quotas can't be checked and the above command throws an error
            pass

        # only check whether a volume is archived if it's smaller than 100MiB,
        # any larger than that and it's very likely to still be in use
        if (group.usage < 100*1024**2):
            try:
                group.archived_dirs = scanDirectory(GROUP_DIRECTORIES[volume][0] +
                                                    group.group_name)
            except FileNotFoundError:
                pass

            # only test the next directory if .imirrored wasn't already found
            if group.archived_dirs is None:
                try:
                    group.archived_dirs = scanDirectory(GROUP_DIRECTORIES[volume][1] +
                                                        group.group_name)
                except FileNotFoundError:
                    pass
        if (group.usage == 0 and group.last_modified == 0):
            # not a useful entry, ignore it
            pass
        else:

            group_data.append(group)

    logger.info("Processed data for {}.".format(volume))

    return (volume, group_data)


def main(start_days_ago: int = 0) -> None:
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    logger.info("Establishing MySQL connection...")
    sql_db = db.common.getSQLConnection(config)

    # Finding most recent wrstat files for each volume
    # We only care if the most recent wrstat file isn't already in the database
    wrstat_files: T.List[str] = []
    wrstat_dates: T.Dict[int, datetime.date] = {}

    for volume in VOLUMES:
        latest_wr = utils.finder.findReport(
            f"scratch{volume}", WRSTAT_DIR, logger, start_days_ago)
        wr_date_str = latest_wr.split("/")[-1].split("_")[0]
        wr_date = datetime.date(int(wr_date_str[:4]), int(
            wr_date_str[4:6]), int(wr_date_str[6:8]))

        if not db.common.check_date(sql_db, "lustre_usage", wr_date, volume, logger):
            wrstat_files.append(latest_wr)
            wrstat_dates[volume] = wr_date

    logger.info("Establishing LDAP connection...")
    ldap_con = utils.ldap.getLDAPConnection()

    logger.info("Collecting group information...")
    pis, groups = utils.ldap.get_humgen_ldap_info(ldap_con)

    logger.info("Starting wrstat processors...")

    # creates a process pool which will concurrently execute 5 processes
    # to read each wrstat file
    # distribute input files to processes running instances of process_wrstat()
    try:
        with multiprocessing.Pool(processes=max(len(wrstat_files), 1)) as pool:
            wr_data = pool.starmap(get_group_data_from_wrstat, zip(
                wrstat_files, repeat(pis), repeat(groups), repeat(logger)))
    except Exception as e:
        sql_db.close()
        raise e

    group_report_data: T.Dict[str, T.List[GroupReport]] = {}
    for volume, group_reports in wr_data:
        group_report_data[volume] = group_reports

    date = datetime.date.today().strftime("%Y-%m-%d")

    logger.info("Transferring report data to MySQL database...")
    db.report.load_usage_report_to_sql(
        sql_db, group_report_data, wrstat_dates, logger)

    logger.info("Writing report data to .tsv file...")
    utils.tsv.createTsvReport(group_report_data, date, REPORT_DIR, logger)

    logger.info("Cleaning up...")
    sql_db.close()
    logger.info("Done.")


if __name__ == "__main__":
    main()
