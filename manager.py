#!/usr/bin/env python3

import os
import sys

import typing as T

from directory_config import MPISTAT_DIR, REPORT_DIR, VOLUMES


def all_exists(mpi_date: str) -> bool:
    # For mpi_date, does a .dat.gz exist for every volume
    for volume in VOLUMES:
        if not os.path.isfile(f"{MPISTAT_DIR}{mpi_date}_{volume}.dat.gz"):
            return False
    return True


def main(modes: T.Set[str]) -> None:
    # Remove any leftover sqlite files
    try:
        os.remove(f"{REPORT_DIR}_lurge_tmp_sqlite.db")
    except FileNotFoundError:
        pass

    if "reporter" in modes:
        # Run report generator
        import report
        report.main()

    if "inspector" in modes:
        import project_inspector
        project_inspector.main(tosql=True)

    if "puppeteer" in modes:
        import puppeteer
        puppeteer.main()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit("Running modes must be provided, inspector, reporter, puppeteer")
    for arg in sys.argv[1:]:
        if arg not in ["reporter", "inspector", "puppeteer"]:
            sys.exit("Available running modes are inspector, reporter, puppeteer")
    main(set(sys.argv[1:]))
