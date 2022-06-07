import csv
from datetime import datetime
from email.headerregistry import Group
from lurge_types.group_report import GroupReport
from directory_config import REPORT_DIR
import logging
import logging.config
from lurge_types.user import UserReport
import typing as T


def createTsvReport(group_reports: T.Dict[str, T.List[GroupReport]], date: str, report_dir: str, logger: logging.Logger) -> None:
    """
    Reads the contents of tables in tmp_db and writes them to a .tsv formatted
    file.

    :param group_reports: volume -> list of GroupReports for all the data we'll put in the report
    :param date: Date string of the data to be used (ie, "2019-09-20")
    """
    # sets filename to 'report-YYYYMMDD.tsv'
    name = "report-{}.tsv".format(date.replace("-", ""))

    with open(report_dir+"report-output-files/"+name, "w", newline="") as reportfile:
        # start a writer that will format the file as tab-separated
        report_writer = csv.writer(reportfile, delimiter="\t",
                                   quoting=csv.QUOTE_NONE)
        # write column headers
        report_writer.writerow(GroupReport.row_headers)

        logger.info("Adding data to tsv report")
        for volume, reports in group_reports.items():
            logger.debug("Inserting data for {}...".format(volume))
            for report in reports:

                data = ["-" if x == None else x for x in report.row]

                report_writer.writerow(data)

    logger.info("{} created.".format(name))


def create_tsv_user_report(user_reports: T.Dict[int, T.DefaultDict[str, UserReport]], usernames: T.Dict[int, str], user_groups: T.Dict[str, T.List[T.Tuple[str, str]]], logger: logging.Logger) -> None:
    logger.info("Writing user report info to TSV file")
    with open(f"{REPORT_DIR}user-reports/{datetime.today().strftime('%Y-%m-%d')}.tsv", "w", newline="") as rf:
        writer = csv.writer(rf, delimiter="\t", quoting=csv.QUOTE_NONE)
        writer.writerow(["username", "data", *user_reports.keys()])

        for uid, uname in usernames.items():
            for grp_name, gid in user_groups[str(uid)]:

                writer.writerow([uname, "size", grp_name, *[
                    round(user_reports[vol][str(uid)].size[gid]/2 ** 20, 2)
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
