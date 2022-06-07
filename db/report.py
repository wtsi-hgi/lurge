import datetime
from lurge_types.group_report import GroupReport
from db_config import SCHEMA
import logging
import typing as T

import mysql.connector

import db.foreign


def load_usage_report_to_sql(sql_db: mysql.connector.MySQLConnection, group_reports: T.Dict[str, T.List[GroupReport]], wrstat_dates: T.Dict[int, datetime.date], logger: logging.Logger):
    """
    Reads the contents of tables in tmp_db and writes them to a MySQL database.

    :param sql_db: MySQL database into which to write data
    :param group_reports: Volumes -> List of group reports - all the data we'll use
    :param tables: List of table names to read
    :param date: Date string to label the data (ie, "2019-09-20")
    """
    sql_cursor = sql_db.cursor()

    # First, get foreign keys from the MySQL database for PI, Volume and Unix Group
    pis, groups, volumes, _, _, _, base_directories = db.foreign.get_db_foreign_keys(
        sql_db, humgen_only=False)

    logger.info("Adding data to MySQL table")
    for volume, reports in group_reports.items():
        logger.debug("Inserting data for {}...".format(volume))
        for report in reports:

            # Making sure the PI, Group and Volume all exist in the DB
            if report.pi_name is not None:
                try:
                    pi = pis[report.pi_name]
                except KeyError:
                    sql_cursor.execute(
                        f"INSERT INTO {SCHEMA}.pi (pi_name) VALUES (%s);", (report.pi_name,))
                    pi = sql_cursor.lastrowid
                    pis[report.pi_name] = pi
            else:
                pi = None

            if report.group_name not in groups:
                sql_cursor.execute(
                    f"INSERT INTO {SCHEMA}.unix_group (group_name) VALUES (%s);", (report.group_name,))
                group_id: int = sql_cursor.lastrowid
                groups[report.group_name] = group_id

            if volume not in volumes:
                sql_cursor.execute(
                    f"INSERT INTO {SCHEMA}.volume (scratch_disk) VALUES (%s);", (volume,))
                volume_id = sql_cursor.lastrowid
                volumes[volume] = volume_id

            if report.base_path not in base_directories:
                sql_cursor.execute(
                    f"INSERT INTO {SCHEMA}.base_directory (directory_path, volume_id) VALUES (%s, %s);",
                    (report.base_path, volumes[volume])
                )
                base_directory_id: int = sql_cursor.lastrowid
                base_directories[report.base_path] = base_directory_id

            # Add our data
            query = f"""INSERT INTO {SCHEMA}.lustre_usage (used, quota, record_date,
                last_modified, pi_id, unix_id, base_directory_id, warning_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

            sql_cursor.execute(query, (
                report.usage,
                report.quota,
                wrstat_dates[int(volume[-3:])],
                report.last_modified_rel,
                pi,
                groups[report.group_name],
                base_directories[report.base_path],
                report.warning
            ))

    sql_db.commit()
    logger.info("Report data loaded into MySQL.")
