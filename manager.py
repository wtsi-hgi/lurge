#!/usr/bin/env python3

import logging
import logging.config
import sys

import typing as T

from directory_config import LOGGING_CONFIG


def main(modes: T.Set[str]) -> None:
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger("manager")

    # Old Reporter and Inspector Merged
    # Don't run from here, run the script using MPI

    if "puppeteer" in modes:
        # Run puppeteer - defaults to all volumes
        logger.info("Running puppeteer")
        import puppeteer
        puppeteer.main()

    if "users" in modes:
        # Run the user_reporter - defaults to all volumes
        logger.info("Running user reporter")
        import user_reporter
        user_reporter.main()

    if "splitter" in modes:
        # Run the group splitter module - defaults to upload to S3
        logger.info("Running group splitter")
        import group_splitter
        group_splitter.main()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.exit(
            "Running modes must be provided, inspector, reporter, puppeteer, users, splitter")
    for arg in sys.argv[1:]:
        if arg not in ["puppeteer", "users", "splitter"]:
            sys.exit(
                "Available running modes are puppeteer, users, splitter")
    main(set(sys.argv[1:]))
