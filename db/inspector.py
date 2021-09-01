import os
import typing as T

import mysql.connector

from utils import finder
from lurge_types.directory_report import DirectoryReport

SCALING_FACTOR = 2**30  # bytes / 2**30 = GiB


def load_inspections_into_sql(db_conn: mysql.connector.MySQLConnection, vol_directory_info: T.Dict[str, T.Dict[str, DirectoryReport]], scratch_disk: str, report_dir: str):
    print("Writing results to MySQL database")

    report_path = finder.findReport(scratch_disk, report_dir)
    mpistat_date = int(os.stat(report_path).st_mtime)

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

    # PI
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.pi")
    pi_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    pis: T.Dict[str, int] = {}
    for (pi_id, pi_name) in pi_results:
        pis[pi_name] = pi_id

    # Groups
    # Unlike in the report, we don't care about if they're part of HumGen or not,
    # so we'll just assume they are. It doesn't matter
    cursor.execute(
        "SELECT group_id, group_name FROM hgi_lustre_usage_new.unix_group WHERE is_humgen = 1")
    group_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    groups: T.Dict[str, int] = {}
    for (group_id, group_name) in group_results:
        groups[group_name] = group_id

    # Volumes
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.volume")
    volume_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    volumes: T.Dict[str, int] = {}
    for (volume_id, volume_name) in volume_results:
        volumes[volume_name] = volume_id

    # Now, we'll go onto the above plan

    # Renaming Old Data
    cursor.execute(
        "UPDATE hgi_lustre_usage_new.directory SET project_name = (SELECT CONCAT('.hgi.old.', project_name));")
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
            _mtime = round((mpistat_date - directory_info[key].mtime)/86400, 1)

            _size = round(directory_info[key].size / SCALING_FACTOR, 2)
            _bam = round(directory_info[key].bam / SCALING_FACTOR, 2)
            _cram = round(directory_info[key].cram / SCALING_FACTOR, 2)
            _vcf = round(directory_info[key].vcf / SCALING_FACTOR, 2)
            _pedbed = round(directory_info[key].pedbed / SCALING_FACTOR, 2)

            _unix_group = directory_info[key].group_name
            _pi = directory_info[key].pi
            _volume = directory_info[key].scratch_disk.split("/")[1]

            # Adding foreign keys if they don't exist
            if _pi is not None:
                try:
                    db_pi = pis[_pi]
                except KeyError:
                    cursor.execute(
                        "INSERT INTO hgi_lustre_usage_new.pi (pi_name) VALUES (%s);", (_pi,))
                    cursor.execute(
                        "SELECT pi_id FROM hgi_lustre_usage_new.pi WHERE pi_name = %s", (_pi,))
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
                        "INSERT INTO hgi_lustre_usage_new.unix_group (group_name, is_humgen) VALUES (%s, %s);", (_unix_group, 1))
                    cursor.execute(
                        "SELECT group_id FROM hgi_lustre_usage_new.unix_group WHERE group_name = %s AND is_humgen = %s;", (_unix_group, 1))
                    (new_group_id,) = cursor.fetchone()
                    groups[_unix_group] = new_group_id
                    db_group = new_group_id
            else:
                db_group = None

            if _volume not in volumes:
                cursor.execute(
                    "INSERT INTO hgi_lustre_usage_new.volume (scratch_disk) VALUES (%s);", (_volume,))
                cursor.execute(
                    "SELECT volume_id FROM hgi_lustre_usage_new.volume WHERE scratch_disk = %s;", (_volume,))
                (new_volume_id,) = cursor.fetchone()
                volumes[_volume] = new_volume_id

            # Add new data
            query = """INSERT INTO hgi_lustre_usage_new.directory (project_name, directory_path, num_files,
            size, last_modified, pi_id, volume_id, group_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

            cursor.execute(query, (
                _project,
                _path,
                _files,
                _size,
                _mtime,
                db_pi,
                volumes[_volume],
                db_group
            ))

            # Get the new directory_id back, so we can add file types
            new_id = cursor.lastrowid

            # Add the file sizes
            # Although these are in the DB with foreign keys, we'll hardcode them here

            """
            Key Filetype
            1   BAM
            2   CRAM
            3   VCF
            4   PEDBED
            """

            cursor.execute("""INSERT INTO hgi_lustre_usage_new.file_size (directory_id, filetype_id, size) 
            VALUES (%s, %s, %s), (%s, %s, %s), (%s, %s, %s), (%s, %s, %s);""", (
                new_id, 1, _bam, new_id, 2, _cram, new_id, 3, _vcf, new_id, 4, _pedbed
            ))

            db_conn.commit()

    # Now we've added all the new data, we can delete all the old data
    # This is data where the project is prefixed with `.hgi.old.`

    cursor.execute("""DELETE FROM hgi_lustre_usage_new.file_size WHERE directory_id IN (
                        SELECT directory_id FROM hgi_lustre_usage_new.directory
                        WHERE project_name LIKE '.hgi.old.%'
                    )""")

    cursor.execute(
        "DELETE FROM hgi_lustre_usage_new.directory WHERE project_name LIKE '.hgi.old%'")

    db_conn.commit()

    # and we're done.
    print("Added data to MySQL Database")
