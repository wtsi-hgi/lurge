from __future__ import annotations

import base64
import gzip
import logging
import os
import re
import typing as T

from mpi4py import MPI

import db.common
import db.inspector
import db_config as config
import utils.finder
import utils.ldap
import utils.tsv
from directory_config import FILETYPES, LOGGING_CONFIG, VOLUMES, WRSTAT_DIR
from lurge_types.directory_report import DirectoryReport
from utils.symlink import get_mdt_symlink

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
WORKERS_PER_VOLUME = 6


def wrstat_reader_worker(
        base_directory_info: T.Set[T.Tuple[str, str]], volume: int):
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

    new_directory_reports: T.Dict[T.Tuple[int, str, str], DirectoryReport] = {}

    controller_rank: int = (
        (rank - len(VOLUMES) - 1) // WORKERS_PER_VOLUME) + 1

    while True:
        comm.send(rank, dest=controller_rank)
        data = comm.recv(source=controller_rank)
        if data["msg"] == "DATA":
            line_block = data["data"]

            for line in line_block:

                line_info = line.split()

                try:
                    path = base64.b64decode(line_info[0]).decode(
                        "UTF-8", "replace")
                except BaseException:
                    continue

                gid = int(line_info[3])

                for grp_dir in base_directory_info:
                    if grp_dir[0] == line_info[3] and path.startswith(
                            grp_dir[1]):
                        base_path = grp_dir[1]
                        break
                else:
                    continue

                _subdir_split = path.replace(base_path, "").split("/")[1:3]
                if len(_subdir_split) == 2 and _subdir_split[0] == "users":
                    subdir = f"users/{_subdir_split[1]}"
                elif len(_subdir_split) == 0:
                    continue  # TODO this isn't right
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
                    new_directory_reports[(
                        gid, base_path, subdir)].size += size
                    new_directory_reports[(
                        gid, base_path, subdir)].num_files += 1

                    if mtime > new_directory_reports[(
                            gid, base_path, subdir)].mtime:
                        new_directory_reports[(
                            gid, base_path, subdir)].mtime = mtime

                    # Filetype Sizes
                    for filetype, regex in FILETYPES.items():
                        if re.compile(regex).search(path):
                            new_directory_reports[(
                                gid, base_path, subdir)].filetypes[filetype] += size

        elif data["msg"] == "DONE":
            comm.send(new_directory_reports, dest=controller_rank)
            return


def get_directory_info_from_wrstat(
    volume: int,
    names: T.Tuple[T.Dict[int, str], T.Dict[int, str]]
) -> None:
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
    base_directory_info = utils.finder.read_base_directories(
        "somepath")  # TODO

    # Format paths and find the wrstat report
    report_path = utils.finder.findReport(
        f"/lustre/scratch{volume}", WRSTAT_DIR, None)
    wrstat_date = int(os.stat(report_path).st_mtime)

    # Reading over every line in the wrstat report
    lines_read = 0

    for worker in range(len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * (rank - 1)),
                        len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * rank)):
        print(f"rank {rank} sending data to rank {worker}")
        comm.send({
            "base_directories": base_directory_info,
            "volume": volume
        }, dest=worker)

    with gzip.open(report_path, "rt") as wrstat:
        _line_block: T.Set[str] = set()
        for line in wrstat:
            lines_read += 1
            if lines_read % 5000000 == 0:
                print(f"read {lines_read} from {volume}")
            _line_block.add(line)

            if len(_line_block) == 250:

                msg = comm.recv()
                comm.send({
                    "msg": "DATA",
                    "data": _line_block
                }, dest=msg)

                _line_block = set()

    # (gid, base_path, directory)
    new_directory_reports: T.Dict[T.Tuple[int, str, str], DirectoryReport] = {}

    for worker in range(len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * (rank - 1)),
                        len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * rank)):
        comm.send({"msg": "DONE"}, dest=worker)
        result = None
        while not isinstance(result, dict):
            result: T.Union[None, int, T.Dict[T.Tuple[int, str,
                                                      str], DirectoryReport]] = comm.recv(source=worker)

        for id, report in result.items():
            if id not in new_directory_reports:
                new_directory_reports[id] = report
            else:
                new_directory_reports[id].size += report.size
                new_directory_reports[id].num_files += report.num_files
                new_directory_reports[id].mtime = max(
                    report.mtime, new_directory_reports[id].mtime)

    directory_reports_lst: T.List[DirectoryReport] = []
    for key, report in new_directory_reports.items():
        report.pi = pis.get(key[0])
        report.group_name = groups.get(key[0])
        report.base_path = get_mdt_symlink(key[1])
        report.subdir = key[2]
        report.relative_mtime = round((wrstat_date - report.mtime) / 86400, 1)

        directory_reports_lst.append(report)

    comm.send(directory_reports_lst, dest=0)


def main() -> None:
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    # LDAP Information
    ldap_con = utils.ldap.getLDAPConnection()
    group_pi_names = utils.ldap.get_groups_ldap_info(ldap_con)

    for idx, vol in enumerate(VOLUMES):
        comm.send({
            "volume": vol,
            "group_pi_names": group_pi_names
        }, dest=idx + 1)

    mappings: T.List[T.List[DirectoryReport]] = []
    for idx, _ in enumerate(VOLUMES):
        mappings.append(comm.recv(source=idx + 1))

    # Write to MySQL database
    db_conn = db.common.getSQLConnection(config)
    db.inspector.load_inspections_into_sql(
        db_conn, [y for x in mappings for y in x], logger)

    # Writing to TSV
    utils.tsv.create_tsv_inspector_report(
        [y for x in mappings for y in x], logger)


if __name__ == "__main__":
    if rank == 0:
        # main process
        main()
    elif rank <= len(VOLUMES):
        # main process for each volume
        data = comm.recv(source=0)
        get_directory_info_from_wrstat(data["volume"], data["group_pi_names"])

    else:
        # any of the worker processes
        data = comm.recv()
        wrstat_reader_worker(data["base_directories"], data["volume"])
