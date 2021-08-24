import gzip
import base64
import sys
import typing as T

import db.common
import db.puppeteer
import utils.finder

import report_config as config

from lurge_types.vault import VaultPuppet
from directory_config import VOLUMES, MPISTAT_DIR

# If Vault has Enter Sandman references in, I'm putting Master of Puppets references here,
# because its the only other Metallica song I know


def processVault(report_path: str) -> T.Dict[str, VaultPuppet]:
    master_of_puppets: T.Dict[str, VaultPuppet] = {}

    with gzip.open(report_path, "rt") as mpistat:
        lines_read: int = 0
        for line in mpistat:

            # Logging
            lines_read += 1
            if lines_read % 5000000 == 0:
                print(
                    f"Read {lines_read} lines from {report_path}", flush=True)

            # Decode the Path, Split it and See If We Care
            mpi_line_info = line.split()

            try:
                filepath = base64.b64decode(
                    mpi_line_info[0]).decode("UTF-8", "replace")
            except:
                continue

            path_elems = filepath.split("/")
            # we need a file in a .vault directory, and a `-` in the last
            # part of the filename, so we know its a full file
            if ".vault" in path_elems and "-" in path_elems[-1]:
                vault_loc = path_elems.index(".vault")
                try:
                    rel_path = base64.b64decode(
                        path_elems[-1].split("-")[1]).decode("UTF-8", "replace")
                except:
                    # TODO
                    continue

                full_path = "/".join(path_elems[:vault_loc]) + "/" + rel_path

                master_of_puppets[full_path] = VaultPuppet(
                    path_elems[vault_loc + 1])

    return master_of_puppets


def main(volumes: T.List[int] = VOLUMES) -> None:
    vault_reports: T.Dict[int, T.Dict[str, VaultPuppet]] = {}
    # TODO: Multiprocessing
    for volume in volumes:
        # Find path to pass
        path = utils.finder.findReport(f"scratch{volume}", MPISTAT_DIR)
        vault_reports[volume] = processVault(path)

    # Write to MySQL database
        db_conn = db.common.getSQLConnection(config)
        db.puppeteer.write_to_db(db_conn, vault_reports)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        try:
            volumes = [int(x) for x in sys.argv[1:]]
            main(volumes)
        except ValueError:
            sys.exit("Arguments provided must be integers for volumes to search")
