from db_config import SCHEMA
import logging
import os
import typing as T

import mysql.connector

import db.foreign
from utils import finder
from lurge_types.directory_report import DirectoryReport

SCALING_FACTOR = 2**30  # bytes / 2**30 = GiB


def load_inspections_into_sql(db_conn: mysql.connector.MySQLConnection, vol_directory_info: T.Dict[str, T.Dict[str, DirectoryReport]], scratch_disk: str, report_dir: str, logger: logging.Logger):
    logger.info("Writing results to MySQL database")

    report_path = finder.findReport(scratch_disk, report_dir, logger)
    wrstat_date = int(os.stat(report_path).st_mtime)

    """
    Plan

    We need to replace the data in the DB.
    So, we're going to mark all the old data as old first,
    by updating its project name to be prefixed `.hgi.old.`

    Then we're going to add all the new data.

    Then we'll delete all the data with `.hgi.old.` in the project name.

    If something goes horribly wrong, we'll still have the old data,
    it'll just be tagged `.hgi.old.`. Which is fine. But hopefully that won't happen.
    """

    cursor = db_conn.cursor(buffered=True)

    # First, get all the foreign keys for PIs, Volumes and Groups
    # We'll also add any that don't exist later
    pis, groups, volumes, _, _, filetypes = db.foreign.get_db_foreign_keys(
        db_conn)

    # Now, we'll go onto the above plan

    # Renaming Old Data
    cursor.execute(
        f"UPDATE {SCHEMA}.directory SET project_name = (SELECT CONCAT('.hgi.old.', project_name));")
    db_conn.commit()

    # We're now going to go through all the DirectoryReports we have
    for directory_info in vol_directory_info.values():
        paths = list(directory_info.keys())
        paths.sort()

        for key in paths:
            try:
                _project = "/".join(key.split("/")[:2])
            except IndexError:
                _project = None

            _path = "/".join(key.split("/")[2:])
            if _path == "":
                _path = None

            _files = directory_info[key].num_files
            _mtime = round((wrstat_date - directory_info[key].mtime)/86400, 1)

            # We're going to scale and round all our sizes
            directory_info[key].size /= SCALING_FACTOR
            directory_info[key].size = round(directory_info[key].size, 2)

            for filetype, size in directory_info[key].filetypes.items():
                directory_info[key].filetypes[filetype] = round(
                    size / SCALING_FACTOR, 2)

            _unix_group = directory_info[key].group_name
            _pi = directory_info[key].pi
            _volume = directory_info[key].scratch_disk.split("/")[1]

            # Adding foreign keys if they don't exist
            if _pi is not None:
                try:
                    db_pi = pis[_pi]
                except KeyError:
                    cursor.execute(
                        f"INSERT INTO {SCHEMA}.pi (pi_name) VALUES (%s);", (_pi,))
                    cursor.execute(
                        f"SELECT pi_id FROM {SCHEMA}.pi WHERE pi_name = %s", (_pi,))
                    (new_pi_id,) = cursor.fetchone()
                    pis[_pi] = new_pi_id
                    db_pi = new_pi_id
            else:
                db_pi = None

            if _unix_group is not None:
                try:
                    db_group = groups[_unix_group]
                except KeyError:
                    cursor.execute(
                        f"INSERT INTO {SCHEMA}.unix_group (group_name, is_humgen) VALUES (%s, %s);", (_unix_group, 1))
                    cursor.execute(
                        f"SELECT group_id FROM {SCHEMA}.unix_group WHERE group_name = %s AND is_humgen = %s;", (_unix_group, 1))
                    (new_group_id,) = cursor.fetchone()
                    groups[_unix_group] = new_group_id
                    db_group = new_group_id
            else:
                db_group = None

            if _volume not in volumes:
                cursor.execute(
                    f"INSERT INTO {SCHEMA}.volume (scratch_disk) VALUES (%s);", (_volume,))
                cursor.execute(
                    f"SELECT volume_id FROM {SCHEMA}.volume WHERE scratch_disk = %s;", (_volume,))
                (new_volume_id,) = cursor.fetchone()
                volumes[_volume] = new_volume_id

            # Add new data
            query = f"""INSERT INTO {SCHEMA}.directory (project_name, directory_path, num_files,
            size, last_modified, pi_id, volume_id, group_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

            cursor.execute(query, (
                _project,
                _path,
                _files,
                directory_info[key].size,
                _mtime,
                db_pi,
                volumes[_volume],
                db_group
            ))

            # Get the new directory_id back, so we can add file types
            new_id = cursor.lastrowid

            # Add the file sizes
            for filetype, size in directory_info[key].filetypes.items():
                if filetype not in filetypes:
                    cursor.execute(
                        f"INSERT INTO {SCHEMA}.filetype (filetype_name) VALUES (%s);", (filetype,))
                    filetypes[filetype] = cursor.lastrowid

                cursor.execute(f"""INSERT INTO {SCHEMA}.file_size (directory_id, filetype_id, size)
                VALUES (%s, %s, %s);""", (new_id, filetypes[filetype], size))

            db_conn.commit()

    # Now we've added all the new data, we can delete all the old data
    # This is data where the project is prefixed with `.hgi.old.`

    cursor.execute(f"""DELETE FROM {SCHEMA}.file_size WHERE directory_id IN (
                        SELECT directory_id FROM {SCHEMA}.directory
                        WHERE project_name LIKE '.hgi.old.%'
                    )""")

    cursor.execute(
        f"DELETE FROM {SCHEMA}.directory WHERE project_name LIKE '.hgi.old%'")

    db_conn.commit()

    # and we're done.
    logger.info("Added data to MySQL Database")
