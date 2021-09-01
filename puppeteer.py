import base64
import datetime
import gzip
import multiprocessing
import sys
import typing as T

import db.common
import db.puppeteer
import utils.finder
import utils.ldap

import report_config as config

from lurge_types.vault import VaultPuppet
from directory_config import VOLUMES, MPISTAT_DIR

# If Vault has Enter Sandman references in, I'm putting Master of Puppets references here,
# because its the only other Metallica song I know


def processVault(volume: int) -> T.Dict[str, VaultPuppet]:
    report_path = utils.finder.findReport(f"scratch{volume}", MPISTAT_DIR)
    master_of_puppets: T.Dict[str, VaultPuppet] = {}

    # 1st Run to Get Vaults
    with gzip.open(report_path, "rt") as mpistat:
        lines_read: int = 0
        for line in mpistat:

            # Logging
            lines_read += 1
            if lines_read % 5000000 == 0:
                print(
                    f"Read {lines_read} lines from {report_path} - Run 1", flush=True)

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
    with gzip.open(report_path, "rt") as mpistat:
        lines_read = 0
        for line in mpistat:
            # Logging
            lines_read += 1
            if lines_read % 5000000 == 0:
                print(
                    f"Read {lines_read} lines from {report_path} - Run 2", flush=True)

            # Decode the Path, Split it and See If We Care
            mpi_line_info = line.split()

            if int(mpi_line_info[8]) in master_of_puppets:
                """
                mpistat lines
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
                puppet = master_of_puppets[int(mpi_line_info[8])]
                puppet.just_call_my_name(
                    size=int(mpi_line_info[1]),
                    owner=mpi_line_info[2],
                    mtime=int(mpi_line_info[5]),
                    group_id=int(mpi_line_info[3])
                )

    ldap_conn = utils.ldap.getLDAPConnection()
    _, group_info = utils.ldap.get_humgen_ldap_info(ldap_conn)
    for puppet in master_of_puppets.values():
        puppet.pull_your_strings(ldap_conn, group_info)

    print(f"Done reading mpistat twice for {volume}", flush=True)
    return volume, master_of_puppets


def main(volumes: T.List[int] = VOLUMES) -> None:
    # Creating SQL Connection
    db_conn = db.common.getSQLConnection(config)

    # Finding most recent mpistat files for each volume
    # We only care if the most recent mpistat file isn't already in the database
    volumes_to_check: T.List[int] = []
    mpistat_dates: T.Dict[int, datetime.date] = {}

    for volume in volumes:
        latest_mpi = utils.finder.findReport(f"scratch{volume}", MPISTAT_DIR)
        mpi_date_str = latest_mpi.split("/")[-1].split("_")[0]
        mpi_date = datetime.date(int(mpi_date_str[:4]), int(
            mpi_date_str[4:6]), int(mpi_date_str[6:8]))

        if not db.puppeteer.check_report_date(db_conn, mpi_date, volume):
            volumes_to_check.append(volume)
            mpistat_dates[volume] = mpi_date

    with multiprocessing.Pool(processes=len(volumes)) as pool:
        vault_reports: T.List[T.Tuple[int, T.Dict[str, VaultPuppet]]] = pool.map(
            processVault, volumes_to_check)

    # Write to MySQL database
    db.puppeteer.write_to_db(db_conn, vault_reports, mpistat_dates)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        try:
            volumes = [int(x) for x in sys.argv[1:]]
            main(volumes)
        except ValueError:
            sys.exit("Arguments provided must be integers for volumes to search")
