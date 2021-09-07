import base64
import binascii
import datetime
import gzip
import logging
import multiprocessing
import sys
import typing as T

from itertools import repeat
from collections import defaultdict
from directory_config import LOGGING_CONFIG, VOLUMES, WRSTAT_DIR
from lurge_types.user import UserReport

import utils.finder
import utils.ldap
import utils.tsv


def process_wrstat(volume: int, logger: logging.Logger) -> T.DefaultDict[str, UserReport]:
    report_path = utils.finder.findReport(
        f"scratch{volume}", WRSTAT_DIR, logger
    )

    user_reports = defaultdict(UserReport)

    with gzip.open(report_path, "rt") as wrstat:
        lines_read: int = 0
        for line in wrstat:

            # Logging
            lines_read += 1
            if lines_read % 5000000 == 0:
                logger.debug(f"Read {lines_read} lines from {volume}")

            # Split all the line info
            wr_line_info = line.split()

            user_id = wr_line_info[2]
            group_id = wr_line_info[3]

            try:
                user_reports[user_id].size[group_id] += int(
                    wr_line_info[1]) // int(wr_line_info[9])
            except ZeroDivisionError:
                pass

            user_reports[user_id].mtime(int(wr_line_info[5]), group_id)

    logger.info(f"Finished processing {volume}")
    return user_reports


def main(volumes: T.List[int] = VOLUMES) -> None:
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    # TODO: Database or just TSV?

    volumes_to_check: T.List[int] = []
    for volume in volumes:
        # TODO: Do we need to check the date?
        volumes_to_check.append(volume)

    with multiprocessing.Pool(processes=max(len(volumes_to_check), 1)) as pool:
        user_reports = pool.starmap(process_wrstat, zip(
            volumes_to_check, repeat(logger)
        ))

    ldap_conn = utils.ldap.getLDAPConnection()

    volume_user_reports = {}
    for i in range(len(volumes_to_check)):
        volume_user_reports[volumes_to_check[i]] = user_reports[i]

    unique_uids = set([int(x) for y in user_reports for x in y.keys()])
    usernames: T.Dict[int, str] = {}
    user_groups: T.Dict[str, T.List[str]] = {}
    for uid in unique_uids:
        usernames[uid] = utils.ldap.get_username(ldap_conn, uid)
        user_groups[uid] = set([key
                            for vol in user_reports
                            for (user, rep) in vol.items()
                            for key in list(rep.size.keys())
                            if user == str(uid)
                            ])

    utils.tsv.create_tsv_user_report(
        volume_user_reports, usernames, user_groups, logger)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        try:
            volumes = [int(x) for x in sys.argv[1:]]
            main(volumes)
        except ValueError:
            sys.exit("Arguments provided must be integers for volumes to search")
