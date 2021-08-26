import gzip
import base64
from os import path
import sys
import typing as T

import db.common
import db.puppeteer
import utils.finder

import report_config as config

from lurge_types.vault import MPIStatFile, VaultPuppet
from directory_config import VOLUMES, MPISTAT_DIR

# If Vault has Enter Sandman references in, I'm putting Master of Puppets references here,
# because its the only other Metallica song I know


def processVault(report_path: str) -> T.Dict[str, VaultPuppet]:
    master_of_puppets: T.Dict[str, VaultPuppet] = {}
    root: T.Optional[MPIStatFile] = None

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
                if lines_read == 1:
                    root = MPIStatFile.from_mpistat(mpi_line_info)
                else:
                    root.insert_child(MPIStatFile.from_mpistat(mpi_line_info))
            except:
                continue  # base64 can throw error

    vaults: T.Set[MPIStatFile] = MPIStatFile.find_vaults(root)

    # Let's now decode all the vault information
    for vault in vaults:
        trackers: T.Set[MPIStatFile] = MPIStatFile.find_files(vault)
        for tracker in trackers:
            vault_loc = tracker.path_elems.index(".vault")
            try:
                rel_path = base64.b64decode(
                    tracker.path_elems[-1].split("-")[1].replace("_", "/")).decode("UTF-8", "replace")
            except:
                # TODO
                continue

            full_path = "/".join(
                tracker.path_elems[:vault_loc]) + "/" + rel_path

            # Get the inode from vault, to check they match later with the file
            encoded_inode = "".join(
                tracker.path_elems[vault_loc:]).split("-")[0]
            try:
                inode = int(encoded_inode, 16)
            except ValueError:
                # TODO
                continue

            # Find the original file
            try:
                tracked_file = MPIStatFile.find_by_path(root, full_path)
            except FileNotFoundError:
                # TODO
                continue

            if inode != tracked_file.inode:
                # TODO
                continue
            master_of_puppets[inode] = VaultPuppet.from_mpistat(
                tracked_file, tracker.path_elems[vault_loc + 1])

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
