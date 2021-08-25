import datetime
import os
import typing as T

from directory_config import MAX_DAYS_AGO


def findReport(scratch_disk: str, report_dir: str):
    days_ago = 0
    volume = scratch_disk[-3:]
    while days_ago < MAX_DAYS_AGO:
        date = datetime.date.today() - datetime.timedelta(days=days_ago)
        proposed_file = f"{report_dir}{date.strftime('%Y%m%d')}_{volume}.dat.gz"
        if not os.path.isfile(proposed_file):
            days_ago += 1
        else:
            print(
                f"{scratch_disk}: using mpistat output for {date.strftime('%Y%m%d')}")
            return proposed_file


def getParents(directory: str) -> T.List[str]:
    """
        Returns a list of directories parent to parameter directory
    """

    split_dir = directory.split("/")
    return ["/".join(split_dir[:i]) for i in range(1, len(split_dir))]
