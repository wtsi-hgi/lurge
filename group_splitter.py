import argparse
import gzip
import logging
import logging.config
from lurge_types.splitter import GroupSplit
import multiprocessing
import typing as T
from collections import defaultdict
from itertools import repeat

import utils.finder
import utils.ldap

from directory_config import LOGGING_CONFIG, REPORT_DIR, Treeserve, WRSTAT_DIR, VOLUMES

def get_group_info_from_wrstat(volume: int, logger: logging.Logger) -> T.DefaultDict[str, GroupSplit]:
    report = utils.finder.findReport(f"scratch{volume}", WRSTAT_DIR, logger)
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

            group_info[group_id].lines.add(line)

            if wr_line_info[7] == "d":
                group_info[group_id].directory_count += 1

    logger.info(f"finished reading {volume}")
    return group_info

def main(upload: bool = True) -> None:
    print(upload)
    # TODO: Add config to logging config file
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers = False)
    logger = logging.getLogger(__name__)

    with multiprocessing.Pool(processes=max(len(VOLUMES), 1)) as pool:
        reports_by_volume: T.List[T.DefaultDict[str, GroupSplit]] = pool.starmap(get_group_info_from_wrstat, zip(VOLUMES, repeat(logger)))

    ldap_conn = utils.ldap.getLDAPConnection()
    _, groups = utils.ldap.get_humgen_ldap_info(ldap_conn)

    all_group_info: T.DefaultDict[str, GroupSplit] = defaultdict(GroupSplit)
    for volume in reports_by_volume:
        for gid, report in volume.items():
            all_group_info[gid] += report

    logger.info(f"writing files for groups")
    for gid, total_report in all_group_info.items():
        try:
            total_report.group_name = groups[gid]
        except KeyError:
            del total_report[gid]
            continue
        
        logger.debug(f"writing file for {total_report.group_name}")
        with gzip.open(f"{REPORT_DIR}/groups/{total_report.group_name}.dat.gz", "wt") as f:
            f.writelines(total_report.lines)

    logger.info("writing index file")
    with open(f"{REPORT_DIR}/groups/index.txt", "w") as f:
        f.write("Group\tBuild Time (sec)\tMemory Use(bytes)\n")

        for report in all_group_info.values():
            build_time = Treeserve.OVERHEAD_SECS + report.lines / Treeserve.LINES_PER_SECOND
            
            directory_percentage = report.directory_count * 100 / len(report.lines)
            bytes_per_node = [x for x in Treeserve.BYTES_PER_NODE_BY_DIR_PERCENT if directory_percentage < x[0]][0][1]

            memory_use = (report.directory_count * 2 + Treeserve.EXTRA_NODES) * bytes_per_node

            f.write("\t".join([report.group_name, build_time, memory_use]))

    # TODO Upload to S3
    if upload:
        ...

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload", action="store_true", help="Upload the produced data to S3")
    args = parser.parse_args()
    main(args.upload)


    