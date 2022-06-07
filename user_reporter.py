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

import db.common
import db.user_reporter
import utils.finder
import utils.ldap
import utils.tsv
import db_config as config


def get_user_info_from_wrstat(volume: int, logger: logging.Logger) -> T.DefaultDict[str, UserReport]:
    """Reads a wrstat file for a volume, and collates the information by user and group

    :param volume: - which volume we're going to look for the wrstat report for
    :param logger: - logging.Logger object to log to

    :returns: DefaultDict[user_id (str), UserReport]

    Example:
    {
        "uid123456": UserReport{
            size: DefaultDict{
                "group_id123": 4000
            },
            _mtime: DefaultDict{
                "group_id123": datetime.date(2021, 09, 01)
            }
        }
    }

    In a UserReport object, size and mtime are DefaultDict[group id (str), value (int/date)]

    """
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

    db_conn = db.common.getSQLConnection(config)

    volumes_to_check: T.List[int] = []
    wrstat_dates: T.Dict[int, datetime.date] = {}

    for volume in volumes:
        latest_wr = utils.finder.findReport(
            f"scratch{volume}", WRSTAT_DIR, logger)
        wr_date_str = latest_wr.split("/")[-1].split("_")[0]
        wr_date = datetime.date(int(wr_date_str[:4]), int(
            wr_date_str[4:6]), int(wr_date_str[6:8]))

        if not db.common.check_date(db_conn, "user_usage", wr_date, volume, logger):
            volumes_to_check.append(volume)
            wrstat_dates[volume] = wr_date

    with multiprocessing.Pool(processes=max(len(volumes_to_check), 1)) as pool:
        user_reports = pool.starmap(get_user_info_from_wrstat, zip(
            volumes_to_check, repeat(logger)
        ))

    # Get some information from LDAP
    ldap_conn = utils.ldap.getLDAPConnection()
    _, groups = utils.ldap.get_groups_ldap_info(ldap_conn)

    volume_user_reports: T.Dict[int, T.DefaultDict[str, UserReport]] = {}
    for i, rep in enumerate(volumes_to_check):
        volume_user_reports[rep] = user_reports[i]

    # For every user, get their username and the groups they're in
    unique_uids = set([int(x) for y in user_reports for x in y.keys()])
    usernames: T.Dict[int, str] = {}
    user_groups: T.Dict[str, T.List[T.Tuple[str, str]]] = {}
    for uid in unique_uids:
        usernames[uid] = utils.ldap.get_username(ldap_conn, uid)
        user_groups[str(uid)] = set([(groups[key], key)
                                     if key in groups else ("-", key)
                                     for vol in user_reports
                                     for (user, report) in vol.items()
                                     for key in list(report.size.keys())
                                     if user == str(uid)
                                     ])

    # Adding data to DB
    db.user_reporter.load_user_reports_to_db(
        db_conn, volume_user_reports, usernames, user_groups, wrstat_dates, logger)

    # Creating TSV of data
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
