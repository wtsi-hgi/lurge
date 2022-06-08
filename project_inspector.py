import base64
import gzip
from itertools import repeat
import logging
import multiprocessing
import os
import re
import typing as T

import db.common
import db.inspector
import utils.finder
import utils.ldap
import utils.tsv

from lurge_types.directory_report import DirectoryReport

import db_config as config


from directory_config import FILETYPES, VOLUMES, WRSTAT_DIR, LOGGING_CONFIG


def get_directory_info_from_wrstat(
    volume: int,
    names: T.Tuple[T.Dict[int, str], T.Dict[int, str]],
    logger: logging.Logger
) -> T.List[DirectoryReport]:
    """Processes a wrstat file and gives us the detailed directory info

    :param path: - List of paths to root directories to scan from
    :param names: - Tuple of dictionaries mapping group IDs to the group name and PI
    :param depth: - How many levels below the root to scan
    :param logger: - A logging.Logger object to log to

    :returns: Dict[directory path (str), directory information (DirectoryReport)]
    example:
        {
            "/lustre/scratch123/teams/hgi/project_abc": DirectoryReport{
                size: 12345,
                filetypes: {
                    "BAM": 12345,
                    "CRAM": 123
                },
                num_files = 5600,
                mtime = 1631019126,
                pi = "PI Name",
                group_name = "Group Name",
                scratch_disk = "/lustre/scratch123"
            }
        }
    """

    pis, groups = names
    base_directory_info = utils.finder.read_base_directories("somepath") # TODO

    # Format paths and find the wrstat report
    report_path = utils.finder.findReport(f"/lustre/scratch{volume}", WRSTAT_DIR, logger)
    wrstat_date = int(os.stat(report_path).st_mtime)

    new_directory_reports: T.Dict[T.Tuple[int, str, str], DirectoryReport] = {} # (gid, base_path, directory)

    # Reading over every line in the wrstat report
    logger.info(f"Reading wrstat output {report_path}")
    lines_read = 0
    with gzip.open(report_path, "rt") as wrstat:
        for line in wrstat:
            lines_read += 1
            if lines_read % 5000000 == 0:
                logger.debug(
                    f"Read {lines_read} lines from wrstat for {volume}")
            line_info = line.split()

            """
            Line Info (layout from wrstat file)
            Index   Item
            0       File Path (base 64 encoded)
            1       Size (bytes)
            2       Owner (User ID)
            3       Group (Group ID)
            4       Last Accessed Time (Unix)
            5       Last Modified Time (Unix)
            6       Last Changed Time (Unix)
            7       File Type (f = file, d = directory)
            8       Inode ID
            9       Number of Hardlinks
            10      Device ID
            """

            try:
                path = base64.b64decode(line_info[0]).decode(
                    "UTF-8", "replace")
            except:
                continue

            gid = int(line_info[3])

            for grp_dir in base_directory_info:
                if grp_dir[0] == line_info[3] and path.startswith(grp_dir[1]):
                    base_path = grp_dir[1]
                    break
            else:
                continue

            _subdir_split = path.replace(base_path, "").split("/")[1:3]
            if len(_subdir_split) == 2 and _subdir_split[0] == "users":
                subdir = f"users/{_subdir_split[1]}"
            elif len(_subdir_split) == 0:
                continue # TODO this isn't right
            else:
                subdir = _subdir_split[0]

            if line_info[7] == "d":
                if (gid, base_path, subdir) not in new_directory_reports:
                    new_directory_reports[(gid, base_path, subdir)] = DirectoryReport(
                        files=1,
                        mtime=int(line_info[5]),
                        scratch_disk=volume
                    )
            elif line_info[7] == "f":
                mtime = int(line_info[5])
                hardlinks = min(1, int(line_info[9]))
                size = int(line_info[1]) // hardlinks

                if (gid, base_path, subdir) not in new_directory_reports:
                    new_directory_reports[(gid, base_path, subdir)] = DirectoryReport(
                        files=0,
                        mtime=mtime,
                        scratch_disk=volume
                    )

                # Update Values
                new_directory_reports[(gid, base_path, subdir)].size += size
                new_directory_reports[(gid, base_path, subdir)].num_files += 1

                if mtime > new_directory_reports[(gid, base_path, subdir)].mtime:
                    new_directory_reports[(gid, base_path, subdir)].mtime = mtime

                # Filetype Sizes
                for filetype, regex in FILETYPES.items():
                    if re.compile(regex).search(path):
                        new_directory_reports[(gid, base_path, subdir)].filetypes[filetype] += size

    directory_reports_lst: T.List[DirectoryReport] = []
    for key, report in new_directory_reports.items():
        report.pi = pis.get(key[0])
        report.group_name = groups.get(key[0])
        report.base_path = key[1]
        report.subdir = key[2]
        report.relative_mtime = round((wrstat_date - report.mtime)/86400, 1)

        directory_reports_lst.append(report)


    return directory_reports_lst


def main() -> None:
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    # LDAP Information
    ldap_con = utils.ldap.getLDAPConnection()
    group_pi_names = utils.ldap.get_groups_ldap_info(ldap_con)

    # Create a multiprocessing pool to process each wrstat file concurrently
    with multiprocessing.Pool() as pool:
        mappings = pool.starmap(
            get_directory_info_from_wrstat,
            zip(
                VOLUMES,
                repeat(group_pi_names),
                repeat(logger)
            )
        )

    # Write to MySQL database
    db_conn = db.common.getSQLConnection(config)
    db.inspector.load_inspections_into_sql(
        db_conn, [y for x in mappings for y in x], logger)

    # Writing to TSV
    utils.tsv.create_tsv_inspector_report([y for x in mappings for y in x], logger)


if __name__ == "__main__":    
    main()
