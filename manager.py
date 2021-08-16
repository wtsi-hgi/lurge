#!/usr/bin/env python3

import datetime
import errno
import os
import sys
import typing as T

# mpistat output files
MPISTAT_DIR = "/lustre/scratch114/teams/hgi/lustre_reports/mpistat/data/"

# where the reports get generated
REPORT_DIR = "/lustre/scratch115/teams/hgi/lustre-usage/"

# how many days back the script will look for
MAX_DAYS_AGO = 10

# scratch disks to look through
VOLUMES = [114, 115, 118, 119, 123]


def all_exists(mpi_date: str) -> bool:
    # For mpi_date, does a .dat.gz exist for every volume
    for volume in VOLUMES:
        if not os.path.isfile(f"{MPISTAT_DIR}{mpi_date}_{volume}.dat.gz"):
            return False
    return True


def main(mode: str) -> None:
    # Remove any leftover sqlite files
    os.remove(f"{REPORT_DIR}_lurge_tmp_sqlite.db")

    days_ago: int = 0
    success: int = 0
    while (days_ago < MAX_DAYS_AGO and success == 0):

        # Finds the date, formatted as YYYYMMDD, ${days_ago} days ago from today
        mpi_date_dt: datetime.date = datetime.datetime.today() - \
            datetime.timedelta(days=days_ago)
        mpi_date: str = mpi_date_dt.strftime("%Y%m%d")
        print(f"Looking for reports at {mpi_date}...")

        # We're looking for the most recent date with a full set of mpistat outputs
        # The date is part of the filename
        # We start at {todays_date}_*.dat.gz and go back each day
        # We won't go earlier than the most recent mpistat output
        if os.path.isfile(f"{REPORT_DIR}report-output-files/report-{mpi_date}.tsv"):
            success = 2
            print(
                f"Can't find mpistat output more recent than report-{mpi_date}.tsv!")
        else:
            if not all_exists(mpi_date):
                days_ago += 1
            else:
                if mode == "inspector":
                    # Run project_inspector
                    # TODO
                    pass
                    success = 1
                elif mode == "report":
                    filenames: T.List[str] = []

                    # When a full set is found, create some links
                    # `latest-{volume}.dat.gz` in the report directory
                    for volume in VOLUMES:
                        new_link: str = f"{REPORT_DIR}latest-{volume}.dat.gz"
                        try:
                            os.symlink(
                                f"{MPISTAT_DIR}{mpi_date}_{volume}.dat.gz", new_link)
                        except OSError as e:
                            if e.errno == errno.EEXIST:
                                os.remove(new_link)
                                os.symlink(
                                    f"{MPISTAT_DIR}{mpi_date}_{volume}.dat.gz", new_link)

                        filenames.append(f"latest-{volume}.dat.gz")
                    print("Created links to latest reports")

                    # Run report generator
                    import report
                    report.main(mpi_date_dt.strftime("%Y-%m-%d"), filenames)
                    success = 1

                else:
                    raise ValueError

    if days_ago >= MAX_DAYS_AGO:
        print(
            f"No usable mpistat output sets from the last {days_ago} days found!")


if __name__ == "__main__":
    if len(sys.argv) == 2:
        if sys.argv[1] in ["inspector", "report"]:
            main(sys.argv[1])

    sys.exit("Must be run in form: python manager.py {report|inspector}")
