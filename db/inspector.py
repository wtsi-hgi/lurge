import os
import typing as T

import mysql.connector

from ..utils import finder
from ..types.directory_report import DirectoryReport

SCALING_FACTOR = 2**30  # bytes / 2**30 = GiB


def load_inspections_into_sql(db_conn: mysql.connector.MySQLConnection, vol_directory_info: T.Dict[str, T.Dict[str, DirectoryReport]], scratch_disk: str):
    print("Writing results to MySQL database")

    report_path = finder.findReport(scratch_disk)
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

    cursor = db_conn.cursor()

    cursor.execute(
        "UPDATE hgi_lustre_usage_new.directory SET project_name = (SELECT CONCAT('.hgi.old', project_name));")
    db_conn.commit()

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
            _volume = directory_info[key].scratch_disk

            # TODO: Pull down foreign keys, and add all this to DB
            ...
