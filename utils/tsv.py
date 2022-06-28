from __future__ import annotations

import csv
import logging
import logging.config
import typing as T
from datetime import datetime

from directory_config import FILETYPES, REPORT_DIR
from lurge_types.group_report import GroupReport
from lurge_types.user import UserReport
from utils import humanise


def create_tsv_report(group_reports: T.List[T.List[GroupReport]],
                      date: str, report_dir: str, logger: logging.LoggerAdapter[logging.Logger]) -> None:
    """
    Reads the contents of tables in tmp_db and writes them to a .tsv formatted
    file.

    :param group_reports: volume -> list of GroupReports for all the data we'll put in the report
    :param date: Date string of the data to be used (ie, "2019-09-20")
    """
    # sets filename to 'report-YYYYMMDD.tsv'
    name = "report-{}.tsv".format(date.replace("-", ""))

    with open(report_dir + "report-output-files/" + name, "w", newline="") as reportfile:
        # start a writer that will format the file as tab-separated
        report_writer = csv.writer(reportfile, delimiter="\t",
                                   quoting=csv.QUOTE_NONE)
        # write column headers
        report_writer.writerow(GroupReport.col_headers)

        logger.info("Adding data to tsv report")
        for _vol in group_reports:
            for report in _vol:
                report_writer.writerow(
                    [x if x is not None else "-" for x in report.row])

    logger.info("{} created.".format(name))


def create_tsv_user_report(user_reports: T.Dict[int, T.DefaultDict[str, UserReport]], usernames: T.Dict[int, str],
                           user_groups: T.Dict[str, T.Set[T.Tuple[str, str]]], logger: logging.Logger) -> None:
    logger.info("Writing user report info to TSV file")
    with open(f"{REPORT_DIR}user-reports/{datetime.today().strftime('%Y-%m-%d')}.tsv", "w", newline="") as rf:
        writer = csv.writer(rf, delimiter="\t", quoting=csv.QUOTE_NONE)
        writer.writerow(["username", "data", *user_reports.keys()])

        for uid, uname in usernames.items():
            for grp_name, gid in user_groups[str(uid)]:

                writer.writerow([uname, "size", grp_name, *[
                    round(user_reports[vol][str(uid)].size[gid] / 2 ** 20, 2)
                    if str(uid) in user_reports[vol] and gid in user_reports[vol][str(uid)].size else 0
                    for vol in user_reports
                ]])

                writer.writerow([uname, "mtime", grp_name, *[
                    user_reports[vol][str(uid)]._mtime[gid].strftime(
                        '%Y-%m-%d')
                    if str(uid) in user_reports[vol] and gid in user_reports[vol][str(uid)]._mtime else "-"
                    for vol in user_reports
                ]])

    logger.info("Done writing user report info to TSV file")


def create_tsv_inspector_report(
        reports: T.List[T.List[GroupReport]], date: str, logger: logging.LoggerAdapter[logging.Logger]) -> None:
    logger.info("writing inspector info to TSV file")

    _filetypes = sorted(FILETYPES.keys())

    with open(f"{REPORT_DIR}inspector-reports/{date}.tsv", "w", newline="") as rf:
        writer = csv.writer(rf, delimiter="\t", quoting=csv.QUOTE_NONE, escapechar="\\")
        writer.writerow([
            "Base Path",
            "Directory",
            "Size",
            *_filetypes,
            "Num Files",
            "Last Modified",
            "PI",
            "Group"
        ])

        for _vol in reports:
            for report in _vol:
                for subdir, directory_report in report.subdirs.items():
                    writer.writerow([
                        report.base_path or "",
                        subdir or "",
                        str(humanise(directory_report.size)),
                        *[str(humanise(size))
                          for size in [directory_report.filetypes[x] for x in _filetypes]],
                        str(directory_report.num_files),
                        str(directory_report.relative_mtime),
                        report.pi_name or "",
                        report.group_name or ""
                    ])
