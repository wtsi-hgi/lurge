from __future__ import annotations

import argparse
import base64
import datetime
import gzip
import logging
import logging.config
import os
import re
import typing as T
from pathlib import Path

from mpi4py import MPI
import setproctitle

import db.common
import db.group_reporter
import db_config as config
import utils.finder
import utils.ldap
from utils.quota import QuotaReader
import utils.tsv
from directory_config import FILETYPES, REPORT_DIR, VOLUMES, WRSTAT_DIR
from lurge_types.group_report import DirectoryReport, GroupReport

# Setting Up MPI
comm = MPI.COMM_WORLD
rank = comm.Get_rank()

# specify how many workers will be requesting data from wrstat to process
# PER VOLUME. If this is changed, so should the number of CPUs requested on
# the compute farm - see cron.sh
WORKERS_PER_VOLUME = 6

# Setting Up Logging
# When developing, DEBUG level logging should be fine (in production, INFO level
# should be used). However, by setting the environment variable LURGE_SUPER_DEBUG_LOG
# to "1", you can enable even more debugging information, however this is very
# overwheling, as it informs you when the workers request work (which is a lot)
# DEBUG level logging is enabled in the INSTANCE environment variable (set in
# cron.sh) == "dev"
SUPER_DEBUG_LOG_LEVEL = 5
logging.addLevelName(SUPER_DEBUG_LOG_LEVEL, "SUPER-DEBUG")

LOG_LEVEL = SUPER_DEBUG_LOG_LEVEL if os.getenv("LURGE_SUPER_DEBUG_LOG") \
    else logging.DEBUG if os.getenv("INSTANCE") == "dev" \
    else logging.INFO

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LEVEL)

_log_handler = logging.StreamHandler()
_log_handler.setLevel(LOG_LEVEL)

_log_formatter = logging.Formatter(
    "%(asctime)s|%(levelname)s|%(rank)s|%(purpose)s|%(message)s")
_log_handler.setFormatter(_log_formatter)

logger.addHandler(_log_handler)
logger = logging.LoggerAdapter(logger, {"rank": f"Rank {rank}", "purpose": ""})


class LurgeLogger(logging.LoggerAdapter):
    """
    Each LogRecord will have come from a particular MPI process. Its rank
    will be displayed in the log (see the LoggerAdapter), however, we want to
    additionally add the purpose of that process, which we define when we
    create a LurgeLogger object (this extends LoggerAdapter) by adding properties

    We also define the `super_debug` method (see description of super debug above)
    """

    def process(self,
                msg: T.Any,
                kwargs: T.MutableMapping[str, T.Any]
                ) -> tuple[T.Any, T.MutableMapping[str, T.Any]]:
        logger.extra.update(self.extra)  # type: ignore
        return super().process(msg, kwargs)

    def super_debug(self, msg: str):
        self.log(SUPER_DEBUG_LOG_LEVEL, msg)


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

    setproctitle.setproctitle(f"Lurge - Volume {volume} Worker (Rank {rank})")
    _logger = LurgeLogger(
        logger, {
            "purpose": f"Volume {volume} Worker"})  # type: ignore

    new_reports: T.Dict[T.Tuple[int, str], GroupReport] = {}

    controller_rank: int = (
        (rank - len(VOLUMES) - 1) // WORKERS_PER_VOLUME) + 1
    _logger.debug(
        f"I'm Rank {rank} for Volume {volume} - my controller is rank {controller_rank}")

    while True:
        _logger.super_debug("requesting work")
        comm.send(rank, dest=controller_rank)
        data = comm.recv(source=controller_rank)
        if data["msg"] == "DATA":
            # we've received some lines of wrstat file
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

                if (gid, base_path) not in new_reports:
                    new_reports[(gid, base_path)] = GroupReport(
                        volume=volume
                    )

                # Update Size
                new_reports[(gid, base_path)
                            ].usage += int(line_info[1]) // int(line_info[9])

                # Update Last Modified Time
                # this is either the time already in the record,
                # the time from the wrstat (if its newer), but
                # not if its in the future, then we set it to now
                new_reports[(gid, base_path)].last_modified = max(
                    new_reports[(gid, base_path)].last_modified,
                    min(int(line_info[5]),
                        int(datetime.datetime.now().timestamp()))
                )

                # find the subdirectory for this line
                _subdir_split = path.replace(base_path, "").split("/")[1:3]
                if len(_subdir_split) == 0:
                    continue
                elif len(_subdir_split) == 1:
                    if line_info[7] == "d":
                        subdir = _subdir_split[0]
                    else:
                        subdir = "."
                else:
                    # if it's a users or projects directory, we'll go one
                    # level deeper
                    if _subdir_split[0] in ["users", "projects"]:
                        subdir = f"{_subdir_split[0]}/{_subdir_split[1]}"
                    else:
                        subdir = _subdir_split[0]

                # we'll add all the info we can, i.e. size, (this is based
                # on whether it is a directory or a file)
                if line_info[7] == "f":
                    mtime = int(line_info[5])
                    hardlinks = min(1, int(line_info[9]))
                    size = int(line_info[1]) // hardlinks

                    if subdir not in new_reports[(gid, base_path)].subdirs:
                        new_reports[(gid, base_path)].subdirs[subdir] = DirectoryReport(
                            mtime=mtime
                        )

                    # Update Values
                    new_reports[(gid, base_path)].subdirs[subdir].size += size
                    new_reports[(gid, base_path)
                                ].subdirs[subdir].num_files += 1

                    if mtime > new_reports[(gid, base_path)
                                           ].subdirs[subdir].mtime:
                        new_reports[(gid, base_path)
                                    ].subdirs[subdir].mtime = mtime

                    # Filetype Sizes
                    for filetype, regex in FILETYPES.items():
                        if re.compile(regex).search(path):
                            new_reports[(
                                gid, base_path)].subdirs[subdir].filetypes[filetype] += size

        elif data["msg"] == "DONE":
            _logger.debug("Done - sending back data")
            comm.send(new_reports, dest=controller_rank)
            return


def reading_wrstat_controller(
    volume: int,
    names: T.Tuple[T.Dict[int, str], T.Dict[int, str]],
    start_days_ago: int = 0
) -> None:
    """
    controls all the workers for a particular volume (rank <= num of volumes)

    Params:
        - volume: int - the volume to analyse
        - names: Tuple[Dict[int, str], Dict[int, str]] -
            (group_id: pi name, group_id: group_name)

    Generates [GroupReport]
    Example: [
        GroupReport{
            volume: 123,
            group_name: "group_name",
            pi_name: "pi_name",
            base_path: "/lustre/scratch119/humgen/projects/project_a",
            usage: 12345,
            quota: 100000,
            subdirs: {
                "subdir_a": DirectoryReport{
                    num_files: 3000,
                    filetypes: {
                        "BAM": 1200,
                        "CRAM": 500
                    },
                }
            }

        }
    ]

    """

    setproctitle.setproctitle(f"Lurge - Volume {volume} Controller")
    _logger = LurgeLogger(
        logger, {
            "purpose": f"Volume {volume} Controller"})  # type: ignore

    # range of the ranks of workers associated to this controller
    workers = range(len(VOLUMES) + 1 + (WORKERS_PER_VOLUME *
                    (rank - 1)), len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * rank))

    pis, groups = names
    logger.debug(f"reading base directory info for volume {volume}")
    try:
        base_directory_info = utils.finder.read_base_directories(
            Path(WRSTAT_DIR))
    except FileNotFoundError as err:
        _logger.exception(err)
        
        # we won't be able to continue, so let's tidy up
        for worker in workers:
            comm.send({
                "base_directories": set(),
                "volume": volume
            }, dest=worker)
            comm.send({"msg": "DONE"}, dest=worker)
        comm.send([], dest=0)
        raise err

    # send important information to the workers
    for worker in workers:
        _logger.info(f"sending information to rank {worker}")
        comm.send({
            "base_directories": base_directory_info,
            "volume": volume
        }, dest=worker)

    # Format paths and find the wrstat report
    report_path = utils.finder.find_report(
        f"/lustre/scratch{volume}", WRSTAT_DIR, _logger, days_ago=start_days_ago)
    wrstat_date = int(os.stat(report_path).st_mtime)

    # check if the DB already has data for this wrstat report
    # if it does, there's no point going over the file, we're not
    # going to get any new data, so we'll tell the workers we're done,
    # (just so they don't hang waiting to do something), and send an empty
    # array back to the rank 0 process, just so it's not waiting for us
    # to produce some data
    if db.common.check_date(
        db.common.get_sql_connection(config),
        "lustre_usage",
        datetime.date.fromtimestamp(wrstat_date),
        volume,
        _logger,
            True):
        comm.send([], dest=0)
        for worker in workers:
            comm.send({"msg": "DONE"}, dest=worker)
        return

    # Reading over every line in the wrstat report
    lines_read = 0

    # We send workers blocks of 250 lines to process
    # This is so the workers aren't constantly asking for work
    _logger.info(f"reading wrstat file {report_path}")
    with gzip.open(report_path, "rt") as wrstat:
        _line_block: T.Set[str] = set()
        for line in wrstat:
            lines_read += 1
            if lines_read % 5000000 == 0:
                _logger.debug(f"read {lines_read} from {volume}")
            _line_block.add(line)

            if len(_line_block) == 250:
                msg = comm.recv()
                _logger.super_debug(
                    f"Rank {msg} requested work - sending it some")
                comm.send({
                    "msg": "DATA",
                    "data": _line_block
                }, dest=msg)

                _line_block = set()

    # (gid, base_path)
    reports: T.Dict[T.Tuple[int, str], GroupReport] = {}

    # When we've sent the entire wrstat file to workers, we can iterate over every
    # worker listening to this controller, and send a DONE message.
    _logger.info(
        "we're done reading wrstat file - let's let all the workers know")
    for worker in range(len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * (rank - 1)),
                        len(VOLUMES) + 1 + (WORKERS_PER_VOLUME * rank)):
        _logger.debug(
            f"letting rank {worker} know we're done, and waiting for response")
        comm.send({"msg": "DONE"}, dest=worker)

        # Then we can wait for a response from every worker, hopefully containing
        # a dictionary of DirectoryReports
        # We wait until it's of the right form incase a worker sends another request
        # for work instead.
        result = None
        while not isinstance(result, dict):
            result: T.Union[None, int, T.Dict[
                T.Tuple[int, str], GroupReport]] = comm.recv(source=worker)

        # As a single report could have been worked on by different, separate, workers,
        # we combine them when they come in, by totalling the sizes etc.
        for id, report in result.items():
            if id not in reports:
                reports[id] = report
            else:
                reports[id] += report

    # Once we've got all the data from the workers collected, we can fill
    # in some gaps, i.e. group name, and then put the finished reports in
    # a list, and send that back to the rank 0 node
    _logger.info(
        f"we've got all our reports back from workers for {volume}, so now we'll just add a bit more info")

    quota_reader = QuotaReader(volume)

    for key, report in reports.items():
        report.pi_name = pis.get(key[0])
        report.group_name = groups.get(key[0])
        if report.group_name:
            report.quota = quota_reader.get_quota(report.group_name)
        report.wrstat_time = wrstat_date
        report.base_path = key[1]

    _logger.info("done - sending data back to main controller")
    comm.send(list(reports.values()), dest=0)


def main_controller() -> None:
    """carried out by the rank 0 process"""

    setproctitle.setproctitle("Lurge - Main Controller")
    _logger = LurgeLogger(
        logger, {
            "purpose": "Main Controller"})  # type: ignore

    # LDAP Information
    _logger.info("Getting LDAP Information")
    ldap_con = utils.ldap.get_ldap_connection()
    group_pi_names = utils.ldap.get_groups_ldap_info(ldap_con)

    _logger.info("Sending Info to Volume Controllers")
    for idx, vol in enumerate(VOLUMES):
        # send information to all the volume controllers
        _logger.debug(f"Sending to rank {idx+1} (volume {vol})")
        comm.send({
            "volume": vol,
            "group_pi_names": group_pi_names
        }, dest=idx + 1)

    _logger.info("waiting on info from volume controllers")
    all_reports: T.List[T.List[GroupReport]] = []
    for idx, vol in enumerate(VOLUMES):
        # wait for information back from these controllers
        all_reports.append(comm.recv(source=idx + 1))
        _logger.debug(f"got info back from rank {idx + 1} (volume {vol})")

    # Write to MySQL database
    _logger.info("writing to SQL DB")
    db_conn = db.common.get_sql_connection(config)
    db.group_reporter.load_reports_into_db(db_conn, all_reports, _logger)

    # Writing to TSV
    _logger.info("writing data to TSV")
    try:
        date = datetime.date.fromtimestamp(
            next(x._wrstat_time for y in all_reports for x in y)).isoformat()  # type: ignore
        utils.tsv.create_tsv_report(all_reports, date, REPORT_DIR, _logger)
        utils.tsv.create_tsv_inspector_report(all_reports, date, _logger)
    except StopIteration:
        _logger.warning("didn't actually get any data - not writing to TSV")

    _logger.info("Done")


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

    parser = argparse.ArgumentParser()
    parser.add_argument('--start-days-ago', type=int, default=0)
    args = parser.parse_args()

    if rank == 0:
        # main process
        main_controller()

    elif rank <= len(VOLUMES):
        # main process for each volume
        data = comm.recv(source=0)
        reading_wrstat_controller(
            data["volume"],
            data["group_pi_names"],
            start_days_ago=args.start_days_ago)

    else:
        # any of the worker processes
        data = comm.recv()
        wrstat_reader_worker(data["base_directories"], data["volume"])
