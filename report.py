from itertools import repeat
import os
import datetime
import subprocess
import logging
import multiprocessing
import sqlite3
import gzip
import sys
import typing as T

import ldap

import db.common
import db.report
import utils.finder
import utils.ldap
import utils.tsv

import report_config as config

from directory_config import DATABASE_NAME, WRSTAT_DIR, REPORT_DIR, VOLUMES, GROUP_DIRECTORIES


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


def process_wrstat(wr_file: str, logger: logging.Logger) -> T.Tuple[str, T.List[T.Tuple[T.Any, ...]]]:
    """
    Processes a single wrstat output file and writes a table of results to
    SQLite. Intended to be ran multiple times concurrently for multiple files.

    :param wr_file: File name of wrstat output file to process
    """

    tmp_db = sqlite3.connect(DATABASE_NAME)

    db_cursor = tmp_db.cursor()
    db_cursor.execute('''SELECT gidNumber, groupName, PI FROM group_table''')
    result = db_cursor.fetchall()

    volume = f"scratch{wr_file.split('/')[-1].split('.')[0].split('_')[1]}"

    groups: T.Dict[str, T.Dict[str, T.Any]] = {}
    for row in result:
        # rearranging into a more usable format, since each row is in the
        # form (gid, "groupname", "PIname")
        gid, groupName, PIname = row

        # lastmodified is in Unix time and will be converted to "days since
        # last modification" later
        groups[str(gid)] = {'groupName': groupName, 'PIname': PIname, 'volumeSize': 0,
                            'lastModified': 0, 'volume': volume, 'isHumgen': True}

    # lazily reads the wrstat file without unzipping the whole thing first
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
                    f"{lines_processed} records processed for {volume}", flush=True)

            line = line.split()

            gid = line[3]
            try:
                try:
                    groups[gid]['volumeSize'] += int(
                        int(line[1]) / int(line[9]))
                except ZeroDivisionError:
                    # This should almost never happen, but it did! Looks like a
                    # file can get 'stat'ed in the middle of being deleted,
                    # which makes it show a hard link count of 0.
                    pass

                try:
                    # only update the group's last edit time if it's more recent
                    if (int(line[5]) > groups[gid]['lastModified']):
                        # make sure the timestamp isn't in the future
                        now = datetime.datetime.now()
                        now_unix = int(datetime.datetime.timestamp(now))
                        if(now_unix > int(line[5])):
                            groups[gid]['lastModified'] = int(line[5])
                except ValueError:
                    continue
            except KeyError:
                # the first time the group appears, add it to the dictionary
                # its name will be found later
                try:
                    groups[gid] = {'groupName': None, 'PIname': None,
                                   'volumeSize': int(line[1]), 'lastModified': int(line[5]),
                                   'volume': volume, 'isHumgen': False}
                except IndexError:
                    continue

            lines_processed += 1

    # gets the Unix timestamp of when the wrstat file was created
    # int() truncates away the sub-second measurements
    wrstat_date_unix = int(os.stat(wr_file).st_mtime)
    group_data: T.List[T.Tuple[T.Any, ...]] = []
    for gid in groups:
        gidNumber = gid
        groupName: str = groups[gid]['groupName']

        # updates the group names for groups discovered during wrstat crawl
        if (groupName == None):
            # connection times out too quickly to be declared elsewhere
            ldap_con = utils.ldap.getLDAPConnection()
            try:
                result = ldap_con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
                                           ldap.SCOPE_ONELEVEL, "(gidNumber={})".format(gid), ["cn"])
            except ValueError:
                continue
            try:
                groupName = result[0][1]['cn'][0].decode('UTF-8')
            except IndexError:
                # nothing found in LDAP for this group id, skip the rest of
                # this loop
                continue

        PI = groups[gid]['PIname']
        volumeSize = groups[gid]['volumeSize']
        lastModified_unix = groups[gid]['lastModified']
        isHumgen = groups[gid]['isHumgen']
        # wrstat_date_unix - lastModified_unix is the seconds since last
        # modification relative to when the wrstat file was produced
        # divided by 86400 (seconds in a day) to find day difference
        lastModified = round((wrstat_date_unix - lastModified_unix)/86400, 1)

        # lfs quota query is split into a list based on whitespace, and the
        # fourth element is taken as the quota. it's in kibibytes though, so it
        # needs to be multiplied by 1024
        try:
            quota = int(subprocess.check_output(["lfs", "quota", "-gq", groupName,
                                                 "/lustre/{}".format(volume)], encoding="UTF-8").split()[3]) * 1024
        except subprocess.CalledProcessError:
            # some groups don't have mercury as a member, which means their
            # quotas can't be checked and the above command throws an error
            quota = None

        archivedDirs = None
        # only check whether a volume is archived if it's smaller than 100MiB,
        # any larger than that and it's very likely to still be in use
        if (volumeSize < 100*1024**2):
            try:
                archivedDirs = scanDirectory(GROUP_DIRECTORIES[volume][0] +
                                             groupName)
            except FileNotFoundError:
                pass

            # only test the next directory if .imirrored wasn't already found
            if archivedDirs is None:
                try:
                    archivedDirs = scanDirectory(GROUP_DIRECTORIES[volume][1] +
                                                 groupName)
                except FileNotFoundError:
                    pass
        if (volumeSize == 0 and lastModified_unix == 0):
            # not a useful entry, ignore it
            pass
        else:
            group_data.append((gidNumber, groupName, PI, volumeSize, volume,
                              lastModified, quota, archivedDirs, isHumgen))

    logger.info("Processed data for {}.".format(volume), flush=True)

    return (volume, group_data)


def generate_tables(tmp_db: sqlite3.Connection, logger: logging.Logger):
    # Create the temporary sqlite database tables
    db_cursor = tmp_db.cursor()
    for vol in VOLUMES:
        logger.info(f"Creating table scratch{vol}")
        db_cursor.execute(f"""
            CREATE TABLE scratch{vol} (gidNumber INTEGER PRIMARY KEY,
            groupName TEXT, PI TEXT, volumeSize INTEGER, volume TEXT,
            lastModified INTEGER, quota INTEGER,
            archivedDirs TEXT, isHumgen INTEGER)
        """)
        tmp_db.commit()
    logger.info("created tables")


def main() -> None:
    logging.config.fileConfig("logging.conf", disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    # temporary SQLite database used to organise data
    tmp_db = sqlite3.connect(DATABASE_NAME)

    logger.info("Establishing MySQL connection...")
    sql_db = db.common.getSQLConnection(config)

    # Finding most recent wrstat files for each volume
    # We only care if the most recent wrstat file isn't already in the database
    wrstat_files: T.List[str] = []
    wrstat_dates: T.Dict[int, datetime.date] = {}

    for volume in VOLUMES:
        latest_wr = utils.finder.findReport(
            f"scratch{volume}", WRSTAT_DIR, logger)
        wr_date_str = latest_wr.split("/")[-1].split("_")[0]
        wr_date = datetime.date(int(wr_date_str[:4]), int(
            wr_date_str[4:6]), int(wr_date_str[6:8]))

        if not db.report.checkReportDate(sql_db, wr_date, volume):
            wrstat_files.append(latest_wr)
            wrstat_dates[volume] = wr_date

    logger.info("Establishing LDAP connection...")
    ldap_con = utils.ldap.getLDAPConnection()

    logger.info("Collecting group information...")
    utils.ldap.add_humgen_ldap_to_db(ldap_con, tmp_db)

    # Create the tables in the temporary DB
    generate_tables(tmp_db, logger)

    # creates a process pool which will concurrently execute 5 processes
    # to read each wrstat file
    pool = multiprocessing.Pool(processes=5)

    logger.info("Starting wrstat processors...", flush=True)
    # sorts file list alphabetically, so that the volumes are in the same order
    # regardless of how the script arguments are given. This is useful for
    # having an ordered multiprocess output later.
    wrstat_files.sort()

    # distribute input files to processes running instances of process_wrstat()
    try:
        wr_data = pool.starmap(process_wrstat, zip(
            wrstat_files, repeat(logger)))
        pool.close()
        pool.join()
    except Exception as e:
        sql_db.close()
        tmp_db.close()
        os.remove(DATABASE_NAME)
        raise e

    tables = [x[0] for x in wr_data]
    group_data = [x for y in wr_data for x in y[1]]

    db_cursor = tmp_db.cursor()

    logger.info("adding data to temporary database")
    for entry in group_data:
        (gidNumber, groupName, PI, volumeSize, volume, lastModified,
         quota, archivedDirs, isHumgen) = entry
        db_cursor.execute("""INSERT INTO {} (gidNumber, groupName, PI,
                    volumeSize, volume, lastModified, quota, archivedDirs, isHumgen)
                    VALUES (?,?,?,?,?,?,?,?,?)""".format(volume),
                          (gidNumber, groupName,
                           PI, volumeSize, volume, lastModified, quota,
                           archivedDirs, isHumgen)
                          )

    tmp_db.commit()
    logger.info("added data to temporary database")

    date = datetime.date.today().strftime("%Y-%m-%d")

    logger.info("Transferring report data to MySQL database...")
    db.report.load_usage_report_to_sql(
        tmp_db, sql_db, tables, wrstat_dates, logger)

    logger.info("Writing report data to .tsv file...")
    utils.tsv.createTsvReport(tmp_db, tables, date, REPORT_DIR)

    logger.info("Cleaning up...")
    sql_db.close()
    tmp_db.close()
    # delete the on-disk SQLite database file
    os.remove(DATABASE_NAME)
    logger.info("Done.")


if __name__ == "__main__":
    main()
