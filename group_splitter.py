import argparse
import datetime
import gzip
import glob
import logging
import logging.config
import os
from lurge_types.splitter import GroupSplit
import multiprocessing
import typing as T
from collections import defaultdict
from itertools import repeat
import subprocess
import time

import utils.finder
import utils.ldap

from directory_config import LOGGING_CONFIG, REPORT_DIR, Treeserve, WRSTAT_DIR, VOLUMES


def get_group_info_from_wrstat(volume: int, groups: T.Dict[str, str], logger: logging.Logger) -> T.DefaultDict[str, GroupSplit]:
    """processes a wrstat file to get us group information

    :param volume: - the volume we're going to be searching through
    :param groups: - pairs of group ids to the group names
    :param logger: - a logging.Logger object to log to

    :returns: DefaultDict[group_id (str), group_information (GroupSplit)]
    example:
        {
            "12345": GroupSplit{
                lines: [],
                line_count: 0,
                directory_count: 0,
                group_name: "group_name_abc",
                volume: 123
            }
        }
    """

    report = utils.finder.findReport(f"scratch{volume}", WRSTAT_DIR, logger)

    if report is None:
        raise FileNotFoundError(
            f"report for scratch{volume} couldn't be found")

    group_info: T.DefaultDict[str, GroupSplit] = defaultdict(GroupSplit)

    with gzip.open(report, "rt") as wrstat:
        lines_read: int = 0
        for line in wrstat:

            # Logging
            lines_read += 1
            if lines_read % 5000000 == 0:
                logger.debug(f"Read {lines_read} from {volume}")

            # Split all the line info
            wr_line_info = line.split()
            group_id: str = wr_line_info[3]

            if group_info[group_id].volume is None:
                # unfortunately, this has to be like this
                # because we need a constructor for defaultdict
                # that isn't dependent on any variable in this
                # function (like volume) :(
                # normally this isn't the case, but because
                # we're in multiprocessing, the constructor
                # needs to be picklable
                group_info[group_id].volume = volume

            if group_info[group_id].group_name is None:
                try:
                    group_info[group_id].group_name = groups[group_id]
                except KeyError:
                    continue

            group_info[group_id].add_lines(line)
            group_info[group_id].line_count += 1
            if wr_line_info[7] == "d":
                group_info[group_id].directory_count += 1

    logger.info(f"finished reading {volume}")
    return group_info


def main(upload: bool = True) -> None:
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    ldap_conn = utils.ldap.getLDAPConnection()
    _, groups = utils.ldap.get_humgen_ldap_info(ldap_conn)

    date_str = datetime.datetime.now().strftime("%Y%m%d")

    try:
        os.makedirs(
            f"{REPORT_DIR}/groups/{date_str}")
    except IsADirectoryError:
        logger.warning(f"data already exists for {date_str}")
        return

    with multiprocessing.Pool(processes=max(len(VOLUMES), 1)) as pool:
        reports_by_volume: T.List[T.DefaultDict[str, GroupSplit]] = pool.starmap(
            get_group_info_from_wrstat, zip(VOLUMES, repeat(groups), repeat(logger)))

    logger.info("flushing any remaining lines and totalling directory counts")
    all_group_info: T.DefaultDict[str, GroupSplit] = defaultdict(GroupSplit)
    for volume in reports_by_volume:
        for gid, report in volume.items():
            report.flush_lines()
            all_group_info[gid] += report

    logger.info("concatenating all the files")
    gids_to_delete: T.List[str] = []
    for gid, total_report in all_group_info.items():
        try:
            total_report.group_name = groups[gid]
            group_files = glob.glob(
                f"{REPORT_DIR}groups/{date_str}/{groups[gid]}.*.dat.gz")
            os.system(
                f"cat {' '.join(group_files)} > {REPORT_DIR}groups/{date_str}/{groups[gid]}.dat.gz")
            for f in group_files:
                os.remove(f)
        except KeyError:
            gids_to_delete.append(gid)
            continue

    for gid in gids_to_delete:
        del all_group_info[gid]

    logger.info("writing index file")
    with open(f"{REPORT_DIR}groups/{date_str}/index.txt", "w") as f:
        f.write("Group\tBuild Time (sec)\tMemory Use(bytes)\n")

        for report in all_group_info.values():
            build_time = Treeserve.OVERHEAD_SECS + \
                report.line_count // Treeserve.LINES_PER_SECOND

            directory_percentage = report.directory_count * \
                100 / report.line_count
            bytes_per_node = [
                x for x in Treeserve.BYTES_PER_NODE_BY_DIR_PERCENT if directory_percentage <= x[0]][0][1]

            memory_use = (report.directory_count * 2 +
                          Treeserve.EXTRA_NODES) * bytes_per_node

            f.write("\t".join([str(report.group_name), str(
                build_time), str(memory_use)]) + "\n")

    # Write group and passwd files
    os.system(f"getent group > {REPORT_DIR}groups/{date_str}/groupfile")
    os.system(f"getent passwd > {REPORT_DIR}groups/{date_str}/passwdfile")

    if upload:
        logger.info("uploading to s3")
        for _ in range(5):
            proc = subprocess.run(
                ["s3cmd", "sync", f"{REPORT_DIR}groups/{date_str}/", Treeserve.S3_UPLOAD_LOCATION], capture_output=True)
            if proc.returncode == 0:
                logger.info("successfully uploaded to S3")
                break
            else:
                logger.warning("s3cmd sync failed, retrying in two seconds")
                logger.debug(
                    f"{proc.stdout.decode('UTF-8')}\n{proc.stderr.decode('UTF-8')}")
                time.sleep(2)
        else:
            logger.warning(
                "s3cmd sync failed five times in a row. didn't sync")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload", action="store_true",
                        help="Upload the produced data to S3")
    args = parser.parse_args()
    main(args.upload)
