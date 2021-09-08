import base64
import datetime
import gzip
from itertools import repeat
import logging
import multiprocessing
import sys
import typing as T

import db.common
import db.puppeteer
import utils.finder
import utils.ldap

import db_config as config

from lurge_types.vault import VaultPuppet
from directory_config import VOLUMES, WRSTAT_DIR, LOGGING_CONFIG

# If Vault has Enter Sandman references in, I'm putting Master of Puppets references here,
# because its the only other Metallica song I know


def processVault(volume: int, logger: logging.Logger) -> T.Dict[str, VaultPuppet]:
    report_path = utils.finder.findReport(
        f"scratch{volume}", WRSTAT_DIR, logger)
    master_of_puppets: T.Dict[str, VaultPuppet] = {}

    # 1st Run to Get Vaults
    with gzip.open(report_path, "rt") as wrstat:
        lines_read: int = 0
        for line in wrstat:

            # Logging
            lines_read += 1
            if lines_read % 5000000 == 0:
                logger.debug(
                    f"Read {lines_read} lines from {report_path} - Run 1")

            # Decode the Path, Split it and See If We Care
            wr_line_info = line.split()

            try:
                filepath = base64.b64decode(
                    wr_line_info[0]).decode("UTF-8", "replace")
            except:
                continue

            path_elems = filepath.split("/")
            # we need a file in a .vault directory, and a `-` in the last
            # part of the filename, so we know its a full file
            if ".vault" in path_elems and "-" in path_elems[-1]:
                vault_loc = path_elems.index(".vault")
                try:
                    rel_path = base64.b64decode(
                        path_elems[-1].split("-")[1]).decode("UTF-8", "replace").replace("_", "/")
                except:
                    # TODO (also find specific error)
                    continue

                full_path = "/".join(path_elems[:vault_loc]) + "/" + rel_path

                # Grab the inode
                encoded_inode = "".join(
                    path_elems[vault_loc + 2:]).split("-")[0]
                try:
                    inode = int(encoded_inode, 16)
                except ValueError:
                    # TODO
                    continue

                master_of_puppets[inode] = VaultPuppet(
                    full_path=full_path,
                    state=path_elems[vault_loc + 1],
                    inode=inode)

    # 2nd. Run to Get File Information
    with gzip.open(report_path, "rt") as wrstat:
        lines_read = 0
        for line in wrstat:
            # Logging
            lines_read += 1
            if lines_read % 5000000 == 0:
                logger.debug(
                    f"Read {lines_read} lines from {report_path} - Run 2")

            # Decode the Path, Split it and See If We Care
            wr_line_info = line.split()

            if int(wr_line_info[8]) in master_of_puppets:
                """
                wrstat lines
                Index   Item
                0       Filepath (base 64 encoded)
                1       Size (bytes)
                2       Owner (ID)
                3       Group (Group ID)
                ...
                5       Last Modified Time (Unix)
                ...
                8       Inode ID
                ...
                """
                puppet = master_of_puppets[int(wr_line_info[8])]
                puppet.just_call_my_name(
                    size=int(wr_line_info[1]),
                    owner=wr_line_info[2],
                    mtime=int(wr_line_info[5]),
                    group_id=wr_line_info[3]
                )

    ldap_conn = utils.ldap.getLDAPConnection()
    _, group_info = utils.ldap.get_humgen_ldap_info(ldap_conn)
    for puppet in master_of_puppets.values():
        puppet.pull_your_strings(ldap_conn, group_info)

    logger.info(f"Done reading wrstat twice for {volume}")
    return volume, master_of_puppets


def main(volumes: T.List[int] = VOLUMES) -> None:
    logging.config.fileConfig(LOGGING_CONFIG, disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    # Creating SQL Connection
    db_conn = db.common.getSQLConnection(config)

    # Finding most recent wrstat files for each volume
    # We only care if the most recent wrstat file isn't already in the database
    volumes_to_check: T.List[int] = []
    wrstat_dates: T.Dict[int, datetime.date] = {}

    for volume in volumes:
        latest_wr = utils.finder.findReport(
            f"scratch{volume}", WRSTAT_DIR, logger)
        wr_date_str = latest_wr.split("/")[-1].split("_")[0]
        wr_date = datetime.date(int(wr_date_str[:4]), int(
            wr_date_str[4:6]), int(wr_date_str[6:8]))

        if not db.puppeteer.check_report_date(db_conn, wr_date, volume, logger):
            volumes_to_check.append(volume)
            wrstat_dates[volume] = wr_date

    with multiprocessing.Pool(processes=max(len(volumes), 1)) as pool:
        vault_reports: T.List[T.Tuple[int, T.Dict[str, VaultPuppet]]] = pool.starmap(
            processVault, zip(volumes_to_check, repeat(logger)))

    # Write to MySQL database
    db.puppeteer.write_to_db(db_conn, vault_reports, wrstat_dates, logger)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        try:
            volumes = [int(x) for x in sys.argv[1:]]
            main(volumes)
        except ValueError:
            sys.exit("Arguments provided must be integers for volumes to search")
