#!/usr/bin/env python3

import os
import sys

from directory_config import MPISTAT_DIR, REPORT_DIR, VOLUMES


def all_exists(mpi_date: str) -> bool:
    # For mpi_date, does a .dat.gz exist for every volume
    for volume in VOLUMES:
        if not os.path.isfile(f"{MPISTAT_DIR}{mpi_date}_{volume}.dat.gz"):
            return False
    return True


def main(mode: str) -> None:
    # Remove any leftover sqlite files
    try:
        os.remove(f"{REPORT_DIR}_lurge_tmp_sqlite.db")
    except FileNotFoundError:
        pass

    if mode in ["report", "both"]:
        # Run report generator
        import report
        report.main()

    if mode in ["inspector", "both"]:
        import project_inspector
        project_inspector.main(tosql=True)


if __name__ == "__main__":
    if len(sys.argv) == 2 and sys.argv[1] in ["inspector", "report", "both"]:
        main(sys.argv[1])
    else:
        sys.exit(
            "Must be run in form: python manager.py {report|inspector|both}")
