import datetime
import os
import typing as T


def findReport(scratch_disk: str, report_dir: str):
    date = datetime.date.today()

    # This assumes that at some point, there is a file to be found
    # Just getting the numbers of /lustre/scratchXXX
    volume = scratch_disk[-3:]
    while not os.path.isfile(f"{report_dir}{date.strftime('%Y%m%d')}_{volume}.dat.gz"):
        date -= datetime.timedelta(days=1)

    if date != datetime.date.today():
        print(
            f"Couldn't find mpistat output for today, used {date.strftime('%Y%m%d')}")

    return f"{report_dir}{date.strftime('%Y%m%d')}_{volume}.dat.gz"


def getParents(directory: str) -> T.List[str]:
    """
        Returns a list of directories parent to parameter directory
    """

    split_dir = directory.split("/")
    return ["/".join(split_dir[:i]) for i in range(1, len(split_dir))]
