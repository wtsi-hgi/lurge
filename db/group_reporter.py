from __future__ import annotations

import datetime
import logging
import typing as T

import mysql.connector
import mysql.connector.cursor

import db.foreign
from db_config import SCHEMA
from lurge_types.group_report import GroupReport
from utils.symlink import get_mdt_symlink

SCALING_FACTOR = 2**30  # bytes / 2**30 = GiB


def load_reports_into_db(db_conn: mysql.connector.MySQLConnection,
                         reports: T.List[T.List[GroupReport]], logger: logging.LoggerAdapter[logging.Logger]) -> None:
    cursor: mysql.connector.cursor.MySQLCursor = db_conn.cursor(buffered=True)

    # Renaming Old Data
    logger.debug("renaming old directory data for deletion later")
    cursor.execute(
        f"UPDATE {SCHEMA}.directory SET directory_path = (SELECT CONCAT('.hgi.old.', directory_path));")
    db_conn.commit()

    pis, groups, volumes, _, _, filetypes, base_dirs = db.foreign.get_db_foreign_keys(
        db_conn)

    # Add Top Level Reports
    for _vol in reports:
        for report in _vol:

            scratch_disk: str = f"scratch{report.volume}"

            base_dir = get_mdt_symlink(report.base_path or "")
            # Making sure the PI, Group and Volume all exist in the DB
            pi: T.Optional[int]
            if report.pi_name is not None:
                try:
                    pi = pis[report.pi_name]
                except KeyError:
                    logger.info(f"adding PI {report.pi_name} to DB")
                    cursor.execute(
                        f"INSERT INTO {SCHEMA}.pi (pi_name) VALUES (%s);", (report.pi_name,))
                    pi = int(cursor.lastrowid)
                    pis[report.pi_name] = pi
            else:
                pi = None

            group_id: T.Optional[int]
            if report.group_name is not None:
                if report.group_name not in groups:
                    logger.info(f"adding group {report.group_name} to DB")
                    cursor.execute(
                        f"INSERT INTO {SCHEMA}.unix_group (group_name) VALUES (%s);", (report.group_name,))
                    group_id = int(cursor.lastrowid)
                else:
                    group_id = groups[report.group_name]
            else:
                group_id = None

            if scratch_disk not in volumes:
                logger.info(f"adding volume {report.volume} to DB")
                cursor.execute(
                    f"INSERT INTO {SCHEMA}.volume (scratch_disk) VALUES (%s);", (scratch_disk,))
                volume_id: int = cursor.lastrowid
                volumes[scratch_disk] = volume_id

            if base_dir not in base_dirs:
                logger.info(
                    f"adding base directory {base_dir} to DB (volume {report.volume})")
                cursor.execute(
                    f"INSERT INTO {SCHEMA}.base_directory (directory_path, volume_id) VALUES (%s, %s);",
                    (base_dir, volumes[scratch_disk])
                )
                base_directory_id: int = cursor.lastrowid
                base_dirs[base_dir] = base_directory_id

            # Add our data
            logger.debug(
                f"adding data to DB for group {report.group_name}, base_directory {base_dir}")
            query = f"""INSERT INTO {SCHEMA}.lustre_usage (used, quota, record_date,
                last_modified, pi_id, unix_id, base_directory_id, warning_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

            cursor.execute(query, (
                report.usage,
                report.quota,
                datetime.date.fromtimestamp(report.wrstat_time),
                report.relative_mtime,
                pi,
                group_id,
                base_dirs[base_dir],
                report.warning
            ))

            db_conn.commit()

            # Add All the Subdirectory Info
            for subdir, subdir_report in report.subdirs.items():
                # scale and round the sizes
                subdir_report.size /= SCALING_FACTOR
                subdir_report.size = round(subdir_report.size, 2)

                for filetype, size in subdir_report.filetypes.items():
                    subdir_report.filetypes[filetype] = round(
                        size / SCALING_FACTOR, 2)

                # Add new data
                logger.debug(f"adding sub directory info for {subdir}")
                query = f"""INSERT INTO {SCHEMA}.directory (directory_path, num_files,
                size, last_modified, pi_id, base_directory_id, group_id) VALUES (%s, %s, %s, %s, %s, %s, %s);"""

                cursor.execute(query, (
                    subdir,
                    subdir_report.num_files,
                    subdir_report.size,
                    subdir_report.relative_mtime,
                    pi,
                    base_dirs.get(base_dir),
                    group_id
                ))

                # Get the new directory_id back, so we can add file types
                new_id: int = cursor.lastrowid

                # Add the file sizes
                for filetype, size in subdir_report.filetypes.items():
                    if filetype not in filetypes:
                        logger.info(f"adding filetype {filetype} to DB")
                        cursor.execute(
                            f"INSERT INTO {SCHEMA}.filetype (filetype_name) VALUES (%s);", (filetype,))
                        filetypes[filetype] = int(cursor.lastrowid)

                    logger.debug(
                        f"adding filetype {filetype} info for {subdir}")
                    cursor.execute(f"""INSERT INTO {SCHEMA}.file_size (directory_id, filetype_id, size)
                    VALUES (%s, %s, %s);""", (new_id, filetypes[filetype], size))

                db_conn.commit()

    # Now we've added all the new data, we can delete all the old data
    # This is data where the path is prefixed with `.hgi.old.`
    logger.debug("deleting old directory information")
    cursor.execute(f"""DELETE FROM {SCHEMA}.file_size WHERE directory_id IN (
                        SELECT directory_id FROM {SCHEMA}.directory
                        WHERE directory_path LIKE '.hgi.old.%'
                    )""")

    cursor.execute(
        f"DELETE FROM {SCHEMA}.directory WHERE directory_path LIKE '.hgi.old%'")

    db_conn.commit()
