import argparse
import base64
import gzip
from itertools import repeat
import multiprocessing
import pathlib
import re
import typing as T

import utils.finder
import utils.ldap
import utils.table

PROJECT_DIRS = {
    'lustre/scratch115/projects': 'lustre/scratch115/realdata/mdt[0-9]/projects',
    'lustre/scratch119/humgen/projects': 'lustre/scratch119/realdata/mdt[0-9]/projects',
    'lustre/scratch115/teams': 'lustre/scratch115/realdata/mdt[0-9]/teams',
    'lustre/scratch119/humgen/teams': 'lustre/scratch119/realdata/mdt[0-9]/teams'
}

ALL_PROJECTS = {
    '114': ["lustre/scratch114/projects", "lustre/scratch114/teams"],
    '115': ["lustre/scratch115/realdata/mdt[0-9]/projects", "lustre/scratch115/realdata/mdt[0-9]/teams"],
    '118': ["lustre/scratch118/humgen/hgi/projects", "lustre/scratch118/humgen/old-team-data"],
    '119': ["lustre/scratch119/realdata/mdt[0-9]/projects", "lustre/scratch119/realdata/mdt[0-9]/teams"]
}

# Regexs for File Types
BAM = re.compile("\.(bam|sam)(\.gz)?$")
CRAM = re.compile("\.cram(\.gz)?$")
VCF = re.compile("\.(vcf|bcf|gvcf)(\.gz)?$")
PEDBED = re.compile("\.(ped|bed)(\.gz)?$")


class DirectoryReport:
    def __init__(self, files, mtime, scratch_disk):
        self.size = 0
        self.bam = 0
        self.cram = 0
        self.vcf = 0
        self.pedbed = 0
        self.num_files = files
        self.mtime = mtime
        self.pi = None
        self.group_name = None
        self.scratch_disk = scratch_disk


def create_mapping(paths: T.List[str], names: T.Tuple[T.Dict[str, str], T.Dict[str, str]], depth: int) -> T.Dict[str, T.Any]:
    """Returns a dictionary mapping paths to objects of properties

    @param path - List of paths to root directories to scan from
    @param names - Tuple of dictionaries mapping group IDs to the group name and PI
    @param depth - How many levels below the root to scan
    """

    humgen_pis, humgen_groups = names

    segmented_path = paths[0].split("/")
    scratch_disk = "/".join(segmented_path[0:2])
    root_parent = "/".join(segmented_path[0:-1])
    report_path = utils.finder.findReport(scratch_disk)

    directory_reports: T.Dict[str, DirectoryReport] = {}

    print(f"Reading mpistat output {report_path}")
    lines_read = 0
    with gzip.open(report_path, "rt") as mpistat:
        for line in mpistat:
            lines_read += 1
            if lines_read % 20000000 == 0:
                print(f"Read {lines_read} from mpistat for {scratch_disk}")
            line_info = line.split()

            """
            Line Info (layout from mpistat file)
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

            entry_path = base64.b64decode(line_info[0]).decode(
                "UTF-8", "replace").strip("/")

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

            directory = "/".join(short_path.split("/")[:_depth])

            # Directory
            if line_info[7] == "d":

                pi = humgen_pis[line_info[3]
                                ] if line_info[3] in humgen_pis else None
                group = humgen_groups[line_info[3]
                                      ] if line_info[3] in humgen_groups else None

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

                # TODO: work out why this is in old project_inspector
                try:
                    size = int(size / links)
                except ZeroDivisionError:
                    continue

                pi = humgen_pis[line_info[3]
                                ] if line_info[3] in humgen_pis else None
                group = humgen_groups[line_info[3]
                                      ] if line_info[3] in humgen_groups else None

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
                if BAM.search(short_path):
                    directory_reports[directory].bam += size
                    for parent in utils.finder.getParents(directory):
                        directory_reports[parent].bam += size

                elif CRAM.search(short_path):
                    directory_reports[directory].cram += size
                    for parent in utils.finder.getParents(directory):
                        directory_reports[parent].cram += size

                elif VCF.search(short_path):
                    directory_reports[directory].vcf += size
                    for parent in utils.finder.getParents(directory):
                        directory_reports[parent].vcf += size

                elif PEDBED.search(short_path):
                    directory_reports[directory].pedbed += size
                    for parent in utils.finder.getParents(directory):
                        directory_reports[parent].pedbed += size

    return directory_reports


def main(depth: int = 2, mode: str = "project", header: bool = True, tosql: bool = False, path: T.Optional[str] = None) -> None:
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

    directories_info: T.Dict[str, T.Dict[str, T.Any]] = {}

    if path is None:
        with multiprocessing.Pool() as pool:
            mappings = pool.starmap(
                create_mapping,
                zip(
                    ALL_PROJECTS.values(),
                    repeat(humgen_names),
                    repeat(depth)
                )
            )

            for mapping in mappings:
                volume = list(mapping.values()[0]["volume"])
                directories_info[volume] = mapping

    else:
        volume = path.split("/")[1][-3:]
        directories_info[volume] = create_mapping(
            [path], humgen_names, depth)

    if tosql:
        # TODO Write to SQL
        pass
    else:
        # Printing to stdout
        print("Values are in GiB. Last modified is relative to mpistat, so may be a few days off")
        if (mode == "project" and header):
            print("Project\tDirectory\tTotal\tBAM\tCRAM\tVCF\tPED/BED\tFiles\tLast Modified (days)\tPI\tUnix Group\tVolume")
        elif (mode == "general" and header):
            print(
                "Directory\tTotal\tBAM\tCRAM\tVCF\tPED/BED\tFiles\tLast Modified (days)")

        for volume in directories_info:
            utils.table.print_table(directories_info[volume], volume, mode)


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