import datetime
from db_config import SCHEMA
import logging
import sqlite3
import typing as T

import mysql.connector

import db.foreign


def load_usage_report_to_sql(tmp_db: sqlite3.Connection, sql_db: mysql.connector.MySQLConnection, tables: T.List[str], wrstat_dates: T.Dict[int, datetime.date], logger: logging.Logger):
    """
    Reads the contents of tables in tmp_db and writes them to a MySQL database.

    :param tmp_db: SQLite database in which tables are stored
    :param sql_db: MySQL database into which to write data
    :param tables: List of table names to read
    :param date: Date string to label the data (ie, "2019-09-20")
    """
    tmp_cursor = tmp_db.cursor()
    sql_cursor = sql_db.cursor()

    # First, get foreign keys from the MySQL database for PI, Volume and Unix Group
    pis, groups, volumes, _, _ = db.foreign.get_db_foreign_keys(
        sql_db, humgen_only=False)

    # Then, we can go over all the data from the tmp_db and put it into the main db

    # iterates over each row in each SQLite table, and just moves the data over
    # into a single MySQL table
    logger.info("Adding data to MySQL table")
    for table in tables:
        logger.debug("Inserting data for {}...".format(table))
        tmp_cursor.execute('''SELECT volume, PI, groupName, volumeSize, quota,
            lastModified, archivedDirs, isHumgen FROM {}
            ORDER BY volume ASC, PI ASC, groupName ASC'''.format(table))
        for (volume, pi_name, group, size, quota, last_mod, archived, isHumgen) in tmp_cursor:

            # Making sure the PI, Group and Volume all exist in the DB
            if pi_name is not None:
                try:
                    pi = pis[pi_name]
                except KeyError:
                    sql_cursor.execute(
                        f"INSERT INTO {SCHEMA}.pi (pi_name) VALUES (?);", pi_name)
                    sql_cursor.execute(
                        f"SELECT pi_id FROM {SCHEMA}.pi WHERE pi_name = %s", pi_name)
                    (pi,) = sql_cursor.fetchone()
                    pis[pi_name] = pi
            else:
                pi = None

            if group not in groups or isHumgen not in groups[group]:
                sql_cursor.execute(
                    f"INSERT INTO {SCHEMA}.unix_group (group_name, is_humgen) VALUES (%s, %s);", (group, isHumgen))
                sql_cursor.execute(
                    f"SELECT group_id FROM {SCHEMA}.unix_group WHERE group_name = %s AND is_humgen = %s;", (group, isHumgen))
                (group_id,) = sql_cursor.fetchone()
                groups[group][isHumgen] = group_id

            if volume not in volumes:
                sql_cursor.execute(
                    f"INSERT INTO {SCHEMA}.volume (scratch_disk) VALUES (%s);", volume)
                sql_cursor.execute(
                    f"SELECT volume_id FROM {SCHEMA}.volume WHERE scratch_disk = %s;", volume)
                (volume_id,) = sql_cursor.fetchone()
                volumes[volume] = volume_id

            # Add our data
            query = f"""INSERT INTO {SCHEMA}.lustre_usage (used, quota, record_date, archived,
                last_modified, pi_id, unix_id, volume_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

            sql_cursor.execute(query, (
                size,
                quota,
                wrstat_dates[int(volume[-3:])],
                archived is not None,
                last_mod,
                pi,
                groups[group][isHumgen],
                volumes[volume]
            ))

    sql_db.commit()
    logger.info("Report data loaded into MySQL.")
