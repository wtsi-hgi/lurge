import datetime
import glob
import logging
import os
import typing as T

from directory_config import MAX_DAYS_AGO


def findReport(scratch_disk: str, report_dir: str, logger: T.Optional[logging.Logger] = None):
    def _mtime(f):
        return os.stat(f).st_mtime

    days_ago = 0
    volume = scratch_disk[-3:]
    while days_ago < MAX_DAYS_AGO:
        date = datetime.date.today() - datetime.timedelta(days=days_ago)
        matching_files = glob.glob(
            f"{report_dir}{date.strftime('%Y%m%d')}_scratch{volume}.*.*.stats.gz")
        if len(matching_files) == 0:
            days_ago += 1
        else:
            # If there's multiple files, we'll grab the most recently edited
            matching_files.sort(reverse=True, key=_mtime)
            if logger:
                logger.info(
                    f"{scratch_disk}: using wrstat output for {date.strftime('%Y%m%d')}")
            return matching_files[0]


def getParents(directory: str) -> T.List[str]:
    """
        Returns a list of directories parent to parameter directory
    """

    split_dir = directory.split("/")
    return ["/".join(split_dir[:i]) for i in range(1, len(split_dir))]
