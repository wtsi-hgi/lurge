import datetime
import os

def findReport(scratch_disk: str, report_dir: str):
    date = datetime.date.today()

    # This assumes that at some point, there is a file to be found
    volume = scratch_disk[-3:] # Just getting the numbers of /lustre/scratchXXX
    while not os.path.isfile(f"{report_dir}{date.strftime('%Y%m%d')}_{volume}.dat.gz"):
        date -= datetime.timedelta(days = 1)

    if date != datetime.date.today():
        print(f"Couldn't find mpistat output for today, used {date.strftime('%Y%m%d')}")

    return f"{report_dir}{date.strftime('%Y%m%d')}_{volume}.dat.gz"

def getParents(directory: str):
    # TODO
    ...