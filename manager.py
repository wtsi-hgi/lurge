#!/usr/bin/env python3

import os
import sys

import typing as T

from directory_config import REPORT_DIR


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
        # Run the inspector, defaulting to adding to SQL DB
        import project_inspector
        project_inspector.main(tosql=True)

    if "puppeteer" in modes:
        # Run puppeteer - defaults to all volumes
        import puppeteer
        puppeteer.main()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit("Running modes must be provided, inspector, reporter, puppeteer")
    for arg in sys.argv[1:]:
        if arg not in ["reporter", "inspector", "puppeteer"]:
            sys.exit("Available running modes are inspector, reporter, puppeteer")
    main(set(sys.argv[1:]))
