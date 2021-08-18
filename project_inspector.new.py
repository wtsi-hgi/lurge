import argparse
from itertools import repeat
import multiprocessing
import pathlib
import re
import typing as T

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


def create_mapping(path: str, names: T.Dict[str, T.Tuple[str, str]], depth: int) -> T.Dict[str, T.Any]:
    return {}


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

    # TODO
    # Next up is LDAP. utils/ldap.py should be made to handle that
    humgen_names = ...

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
            path, humgen_names, depth)

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
