from __future__ import annotations

import datetime
import glob
import logging
import os
import typing as T
from pathlib import Path

from directory_config import MAX_DAYS_AGO


def find_report(scratch_disk: str, report_dir: str,
                logger: T.Optional[logging.LoggerAdapter[logging.Logger]] = None, days_ago: int = 0) -> str:
    def _mtime(f):
        return os.stat(f).st_mtime

    volume = scratch_disk[-3:]
    while days_ago < MAX_DAYS_AGO:
        date = datetime.date.today() - datetime.timedelta(days=days_ago)
        matching_files = glob.glob(
            f"{report_dir}{date.strftime('%Y%m%d')}_scratch{volume}.*.*.stats.gz")  # wrstat
        # f"{report_dir}{date.strftime('%Y%m%d')}_{volume}.*.*.stats.gz") # mpistat
        if len(matching_files) == 0:
            days_ago += 1
        else:
            # If there's multiple files, we'll grab the most recently edited
            matching_files.sort(reverse=True, key=_mtime)
            if logger:
                logger.info(
                    f"{scratch_disk}: using wrstat output for {date.strftime('%Y%m%d')}")
            return matching_files[0]

    raise FileNotFoundError


def getParents(directory: str) -> T.List[str]:
    """
        Returns a list of directories parent to parameter directory
    """

    split_dir = directory.split("/")
    return ["/".join(split_dir[:i]) for i in range(1, len(split_dir))]


def read_base_directories(report_dir: Path) -> T.Set[T.Tuple[str, str]]:
    with open("/lustre/scratch119/humgen/teams/hgi/users/mg38/small.tsv") as f:  # TODO
        # One Line is a Group:Base Directory Pairing
        # (group being gid)
        # there can be many groups to one base directory,
        # and many base directories to one group
        return {tuple(line.strip().split("\t")) for line in f}
