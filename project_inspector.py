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
    These workers will each be associated to a controller for a particular
    volume (we calculate the rank of what that controller will be).

    When we're ready for work, we send a message to the controller with our
    rank number. It'll send us back a block of 250 lines of wrstat to process

    When we get a DONE message, we send all our reports back to the controller
    
    *** WRSTAT LINE LAYOUT ***
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

                # decode the base64 encoded file path
                try:
                    path = base64.b64decode(line_info[0]).decode(
                        "UTF-8", "replace")
                except BaseException:
                    continue

                gid = int(line_info[3])

                # find the appropriate base directory
                for grp_dir in base_directory_info:
                    if grp_dir[0] == line_info[3] and path.startswith(
                            grp_dir[1]):
                        base_path = grp_dir[1]
                        break
                else:
                    continue

                # find the subdirectory for this line
                # if it's a users directory, we'll go one level deeper
                _subdir_split = path.replace(base_path, "").split("/")[1:3]
                if len(_subdir_split) == 0:
                    continue
                elif len(_subdir_split) == 1:
                    if line_info[7] == "d":
                        subdir = _subdir_split[0]
                    else:
                        subdir = "."
                else:
                    if _subdir_split[0] == "users":
                        subdir = f"users/{_subdir_split[1]}"
                    else:
                        subdir = _subdir_split[0]

                # we'll add all the info we can, i.e. size, (this is based
                # on whether it is a directory or a file)
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


def reading_wrstat_controller(
    volume: int,
    names: T.Tuple[T.Dict[int, str], T.Dict[int, str]]
) -> None:
    """
    controls all the workers for a particular volume (rank <= num of volumes)

    Params:
        - volume: int - the volume to analyse
        - names: Tuple[Dict[int, str], Dict[int, str]] - (group_id: pi name, group_id: group_name)

    Generates List[DirectoryReports] to send back to appropriate controller
    example: [
        DirectoryReport{
            size: 12345,
            num_files: 5600,
            mtime: 1631019126,
            pi: "PI Name",
            group_name: "Group Name",
            base_path: "/lustre/scratch123/teams/hgi/project_abc",
            subdir: "users/ab12",
            relative_mtime: 5
            filetypes: {
                "BAM": 120,
                "CRAM": 340
            }
        }
    ]

    """

    pis, groups = names
    base_directory_info = utils.finder.read_base_directories(
        "somepath")  # TODO

    # Format paths and find the wrstat report
    report_path = utils.finder.find_report(
        f"/lustre/scratch{volume}", WRSTAT_DIR, None)
    wrstat_date = int(os.stat(report_path).st_mtime)

    # Reading over every line in the wrstat report
    lines_read = 0

    for worker in range(len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * (rank - 1)),
                        len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * rank)):
        comm.send({
            "base_directories": base_directory_info,
            "volume": volume
        }, dest=worker)

    # We send workers blocks of 250 lines to process
    # This is so the workers aren't constantly asking for work
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

    # When we've sent the entire wrstat file to workers, we can iterate over every
    # worker listening to this controller, and send a DONE message.
    for worker in range(len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * (rank - 1)),
                        len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * rank)):
        comm.send({"msg": "DONE"}, dest=worker)
        
        # Then we can wait for a response from every worker, hopefully containing
        # a dictionary of DirectoryReports
        # We wait until it's of the right form incase a worker sends another request
        # for work instead.
        result = None
        while not isinstance(result, dict):
            result: T.Union[None, int, T.Dict[T.Tuple[int, str,
                                                      str], DirectoryReport]] = comm.recv(source=worker)

        # As a single report could have been worked on by different, separate, workers,
        # we combine them when they come in, by totalling the sizes etc.
        for id, report in result.items():
            if id not in new_directory_reports:
                new_directory_reports[id] = report
            else:
                new_directory_reports[id].size += report.size
                new_directory_reports[id].num_files += report.num_files
                new_directory_reports[id].mtime = max(
                    report.mtime, new_directory_reports[id].mtime)
                for key, value in report.filetypes.items():
                    new_directory_reports[id].filetypes[key] += value

    # Once we've got all the data from the workers collected, we can fill
    # in some gaps, i.e. group name, and then put the finished reports in 
    # a list, and send that back to the rank 0 node
    directory_reports_lst: T.List[DirectoryReport] = []
    for key, report in new_directory_reports.items():
        report.pi = pis.get(key[0])
        report.group_name = groups.get(key[0])
        report.base_path = get_mdt_symlink(key[1])
        report.subdir = key[2]
        report.relative_mtime = round((wrstat_date - report.mtime) / 86400, 1)

        directory_reports_lst.append(report)

    comm.send(directory_reports_lst, dest=0)


def main_controller() -> None:
    """carried out by the rank 0 process"""

    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    # LDAP Information
    ldap_con = utils.ldap.get_ldap_connection()
    group_pi_names = utils.ldap.get_groups_ldap_info(ldap_con)

    for idx, vol in enumerate(VOLUMES):
        # send information to all the volume controllers
        comm.send({
            "volume": vol,
            "group_pi_names": group_pi_names
        }, dest=idx + 1)

    mappings: T.List[T.List[DirectoryReport]] = []
    for idx, _ in enumerate(VOLUMES):
        # wait for information back from those controllers
        mappings.append(comm.recv(source=idx + 1))

    # Write to MySQL database
    db_conn = db.common.getSQLConnection(config)
    db.inspector.load_inspections_into_sql(
        db_conn, [y for x in mappings for y in x], logger)

    # Writing to TSV
    utils.tsv.create_tsv_inspector_report(
        [y for x in mappings for y in x], logger)


if __name__ == "__main__":

    """
    MPI Ranks
    Rank 0: Process to spin up volume controller processes,
    and eventually write everything to the database.

    Rank <= num of volumes: each of these will act as a 
    controller for the workers for each volume

    Other rank: these will work as a worker, each associated
    to a volume controller rank
    """

    if rank == 0:
        # main process
        main_controller()
    elif rank <= len(VOLUMES):
        # main process for each volume
        data = comm.recv(source=0)
        reading_wrstat_controller(data["volume"], data["group_pi_names"])

    else:
        # any of the worker processes
        data = comm.recv()
        wrstat_reader_worker(data["base_directories"], data["volume"])
