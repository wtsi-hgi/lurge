import argparse
import base64
import gzip
from itertools import repeat
import logging
import multiprocessing
import pathlib
import re
import typing as T

import db.common
import db.inspector
import utils.finder
import utils.ldap
import utils.table

from lurge_types.directory_report import DirectoryReport

import db_config as config


from directory_config import FILETYPES, PSEUDO_GROUPS, WRSTAT_DIR, PROJECT_DIRS, ALL_PROJECTS, LOGGING_CONFIG


def get_directory_info_from_wrstat(paths: T.List[str], names: T.Tuple[T.Dict[str, str], T.Dict[str, str]], depth: int, logger: logging.Logger) -> T.Dict[str, DirectoryReport]:
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

    humgen_pis, humgen_groups = names

    # Format paths and find the wrstat report
    segmented_path = paths[0].split("/")
    scratch_disk = "/".join(segmented_path[0:2])
    root_parent = "/".join(segmented_path[0:-1])
    report_path = utils.finder.findReport(scratch_disk, WRSTAT_DIR, logger)

    directory_reports: T.Dict[str, DirectoryReport] = {}

    # Reading over every line in the wrstat report
    logger.info(f"Reading wrstat output {report_path}")
    lines_read = 0
    with gzip.open(report_path, "rt") as wrstat:
        for line in wrstat:
            lines_read += 1
            if lines_read % 5000000 == 0:
                logger.debug(
                    f"Read {lines_read} lines from wrstat for {scratch_disk}")
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
                entry_path = base64.b64decode(line_info[0]).decode(
                    "UTF-8", "replace").strip("/")
            except:
                continue

            # If the entry path doesn't contain a target directory
            # then we don't care about the entry, and its skipped
            for path in paths:
                if re.match(path, entry_path) is not None:
                    break
            else:
                continue

            short_path = re.sub(root_parent, "", entry_path).strip("/")

            _dir = short_path.split("/")[:-1]

            # Just use the directory if its a file
            if line_info[7] == "f":
                _dir = _dir[:-1]

            # Go a level deeper if users directory
            try:
                _depth = depth + 1 if _dir[2].lower() == "users" else depth
            except IndexError:
                _depth = depth

            directory = "/".join(_dir[:_depth])

            group = None
            pi = None
            for pseudo_group_path in PSEUDO_GROUPS.keys():
                if entry_path.startswith(pseudo_group_path.strip("/")):
                    group = str(PSEUDO_GROUPS[pseudo_group_path][1])
                    pi = str(PSEUDO_GROUPS[pseudo_group_path][2])
                    break
            else:
                pi = humgen_pis.get(line_info[3])
                group = humgen_groups.get(line_info[3])

            # Directory
            if line_info[7] == "d":

                # Create entries for the directory, and all parent directories
                if directory not in directory_reports:
                    directory_reports[directory] = DirectoryReport(
                        files=1, mtime=int(line_info[5]), scratch_disk=scratch_disk)
                    for parent in utils.finder.getParents(directory):
                        if parent not in directory_reports:
                            directory_reports[parent] = DirectoryReport(
                                files=1, mtime=int(line_info[5]), scratch_disk=scratch_disk)

                directory_reports[directory].pi = pi
                directory_reports[directory].group_name = group

            # File
            elif line_info[7] == "f":
                size = int(line_info[1])
                mtime = int(line_info[5])
                links = int(line_info[9])

                try:
                    size = int(size / links)
                except ZeroDivisionError:
                    continue

                # Create entry for file and all parent directories
                if directory not in directory_reports:
                    directory_reports[directory] = DirectoryReport(
                        files=0,
                        mtime=mtime,
                        scratch_disk=scratch_disk
                    )

                    for parent in utils.finder.getParents(directory):
                        if parent not in directory_reports:
                            directory_reports[parent] = DirectoryReport(
                                files=0,
                                mtime=mtime,
                                scratch_disk=scratch_disk
                            )

                # Update Directory Values
                directory_reports[directory].size += size
                directory_reports[directory].num_files += 1
                directory_reports[directory].pi = pi
                directory_reports[directory].group_name = group

                if mtime > directory_reports[directory].mtime:
                    directory_reports[directory].mtime = mtime

                # Update Parents
                for parent in utils.finder.getParents(directory):
                    directory_reports[parent].size += size
                    directory_reports[parent].num_files += 1
                    if mtime > directory_reports[parent].mtime:
                        directory_reports[parent].mtime = mtime

                # Filetype Sizes
                for filetype, regex in FILETYPES.items():
                    if re.compile(regex).search(short_path):
                        directory_reports[directory].filetypes[filetype] += size
                        for parent in utils.finder.getParents(directory):
                            directory_reports[parent].filetypes[filetype] += size

    return directory_reports


def main(depth: int = 2, mode: str = "project", header: bool = True, tosql: bool = False, path: T.Optional[str] = None) -> None:
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    depth = int(depth) + 1

    if path is not None:
        full_path = pathlib.Path(path).resolve()
        full_path = str(full_path).strip("/")

        for key in PROJECT_DIRS:
            if re.match(key, full_path):
                _suffix = re.sub(key, "", full_path)
                full_path = PROJECT_DIRS[key] + _suffix
                break

    # LDAP Information
    ldap_con = utils.ldap.getLDAPConnection()
    humgen_names = utils.ldap.get_humgen_ldap_info(ldap_con)

    directories_info: T.Dict[str, T.Dict[str, DirectoryReport]] = {}

    # Create a multiprocessing pool to process each wrstat file concurrently
    if path is None:
        with multiprocessing.Pool() as pool:
            mappings = pool.starmap(
                get_directory_info_from_wrstat,
                zip(
                    ALL_PROJECTS.values(),
                    repeat(humgen_names),
                    repeat(depth),
                    repeat(logger)
                )
            )

            for mapping in mappings:
                volume = list(mapping.values())[0].scratch_disk
                directories_info[volume] = mapping

    else:
        volume = path.split("/")[1][-3:]
        directories_info[volume] = get_directory_info_from_wrstat(
            [path], humgen_names, depth)

    if tosql:
        # Write to MySQL database
        db_conn = db.common.getSQLConnection(config)
        db.inspector.load_inspections_into_sql(
            db_conn, directories_info, volume, WRSTAT_DIR, logger)
    else:
        # Printing to stdout
        filetype_headers: str = "\t".join(FILETYPES.keys())

        print("Last modified is relative to wrstat, so may be a few days off")
        if (mode == "project" and header):
            print(
                f"Project\tDirectory\tTotal\t{filetype_headers}\tFiles\tLast Modified (days)\tPI\tUnix Group\tVolume")
        elif (mode == "general" and header):
            print(
                f"Directory\tTotal\t{filetype_headers}\tFiles\tLast Modified (days)")

        for volume in directories_info:
            utils.table.print_table(
                directories_info[volume], volume, mode, WRSTAT_DIR, logger)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create a summary of disk usage of a project directory"
    )

    parser.add_argument("--depth", "-d", nargs="?", type=int, default=2,
                        help="The depth of the output. Default: 2")

    parser.add_argument("--generic", dest="mode", action="store_const", const="general", default="project",
                        help="When this flag is used, the output will have the project/directory colums replaced by a single path when outputting a table")

    parser.add_argument("--noheader", dest="header", action="store_const", const=False, default=True,
                        help="Don't print column headers")

    parser.add_argument("--tosql", dest="tosql", action="store_const", const=True, default=False,
                        help="In addition to printing to stdout, this flag will write the output to a MySQL database")

    parser.add_argument(
        "path", nargs="?", help="The path to scan. The final directory in the path is considered the root. Leave empty to scan HGI project directories on different volumes all at the same time.")

    args = parser.parse_args()

    if (args.tosql and args.path is not None):
        print("--tosql flag cannot be used with an explicit path argument!")
    else:
        main(
            args.depth,
            args.mode,
            args.header,
            args.tosql,
            args.path
        )
