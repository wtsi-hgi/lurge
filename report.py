import os
import re
import datetime
import subprocess
import multiprocessing
import sqlite3
import gzip
import sys
import typing as T

import ldap

import db.common
import db.report
import utils.finder
import utils.ldap
import utils.tsv

import report_config as config

from directory_config import DATABASE_NAME, MPISTAT_DIR, REPORT_DIR, VOLUMES


def scanDirectory(directory: str) -> T.Optional[str]:
    """
    processMpistat helper function. Scans 'directory' for .imirrored, and returns
    the directory if it was found and None otherwise.

    :param directory: Directory to scan
    """
    with os.scandir(directory) as items:
        for item in items:
            if item.name == ".imirrored":
                return directory

    return None


def processMpistat(mpi_file: str) -> T.Tuple[str, T.List[T.Tuple[T.Any, ...]]]:
    """
    Processes a single mpistat output file and writes a table of results to
    SQLite. Intended to be ran multiple times concurrently for multiple files.

    :param mpi_file: File name of mpistat output file to process
    """

    tmp_db = sqlite3.connect(DATABASE_NAME)

    db_cursor = tmp_db.cursor()
    db_cursor.execute('''SELECT gidNumber, groupName, PI FROM group_table''')
    result = db_cursor.fetchall()

    volume = f"scratch{mpi_file.split('/')[-1].split('.')[0].split('_')[1]}"

    groups: T.Dict[str, T.Dict[str, T.Any]] = {}
    for row in result:
        # rearranging into a more usable format, since each row is in the
        # form (gid, "groupname", "PIname")
        gid, groupName, PIname = row

        # lastmodified is in Unix time and will be converted to "days since
        # last modification" later
        groups[str(gid)] = {'groupName': groupName, 'PIname': PIname, 'volumeSize': 0,
                            'lastModified': 0, 'volume': volume, 'isHumgen': True}

    # lazily reads the mpistat file without unzipping the whole thing first
    starttime = datetime.datetime.now()
    lines_processed = 0
    print("Opening {} for reading...".format(mpi_file))
    # directories with group directories to scan for .imirrored
    group_directories: T.Dict[str, T.List[str]] = {
        'scratch114': ["/lustre/scratch114/teams/", "/lustre/scratch114/projects/"],
        'scratch115': ["/lustre/scratch115/teams/", "/lustre/scratch115/projects/"],
        'scratch118': ["/lustre/scratch118/humgen/old-team-data/",
                       "/lustre/scratch118/humgen/hgi/projects/"],
        'scratch119': ["/lustre/scratch119/humgen/teams",
                       "/lustre/scratch119/humgen/projects/"],
        "scratch123": ["/lustre/scratch123/hgi/teams/", "/lustre/scratch123/hgi/projects/"]
    }

    with gzip.open(mpi_file, 'rt') as mpi_text:
        # each line in the mpistat file has the following whitespace separated
        # fields:
        # base64 encoded path, file size, owner uid, owner gid, last access time,
        # last modification time, last status change time, object type (file,
        # directory or link), inode number, number of links, device id
        for line in mpi_text:

            # print out progress report every ~30 seconds
            if (datetime.datetime.now() - starttime).seconds > 30:
                starttime = datetime.datetime.now()
                print("{:%H:%M:%S}: {} records processed for {}".format(
                    starttime, lines_processed, volume), flush=True)

            line = line.split()

            gid = line[3]
            try:
                try:
                    groups[gid]['volumeSize'] += int(
                        int(line[1]) / int(line[9]))
                except ZeroDivisionError:
                    # This should almost never happen, but it did! Looks like a
                    # file can get 'stat'ed in the middle of being deleted,
                    # which makes it show a hard link count of 0.
                    pass

                try:
                    # only update the group's last edit time if it's more recent
                    if (int(line[5]) > groups[gid]['lastModified']):
                        # make sure the timestamp isn't in the future
                        now = datetime.datetime.now()
                        now_unix = int(datetime.datetime.timestamp(now))
                        if(now_unix > int(line[5])):
                            groups[gid]['lastModified'] = int(line[5])
                except ValueError:
                    continue
            except KeyError:
                # the first time the group appears, add it to the dictionary
                # its name will be found later
                try:
                    groups[gid] = {'groupName': None, 'PIname': None,
                                   'volumeSize': int(line[1]), 'lastModified': int(line[5]),
                                   'volume': volume, 'isHumgen': False}
                except IndexError:
                    continue

            lines_processed += 1

    # gets the Unix timestamp of when the mpistat file was created
    # int() truncates away the sub-second measurements
    mpistat_date_unix = int(os.stat(mpi_file).st_mtime)
    group_data: T.List[T.Tuple[T.Any, ...]] = []
    for gid in groups:
        gidNumber = gid
        groupName: str = groups[gid]['groupName']

        # updates the group names for groups discovered during mpistat crawl
        if (groupName == None):
            # connection times out too quickly to be declared elsewhere
            ldap_con = utils.ldap.getLDAPConnection()
            try:
                result = ldap_con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
                                           ldap.SCOPE_ONELEVEL, "(gidNumber={})".format(gid), ["cn"])
            except ValueError:
                continue
            try:
                groupName = result[0][1]['cn'][0].decode('UTF-8')
            except IndexError:
                # nothing found in LDAP for this group id, skip the rest of
                # this loop
                continue

        PI = groups[gid]['PIname']
        volumeSize = groups[gid]['volumeSize']
        lastModified_unix = groups[gid]['lastModified']
        isHumgen = groups[gid]['isHumgen']
        # mpistat_date_unix - lastModified_unix is the seconds since last
        # modification relative to when the mpistat file was produced
        # divided by 86400 (seconds in a day) to find day difference
        lastModified = round((mpistat_date_unix - lastModified_unix)/86400, 1)

        # lfs quota query is split into a list based on whitespace, and the
        # fourth element is taken as the quota. it's in kibibytes though, so it
        # needs to be multiplied by 1024
        try:
            quota = int(subprocess.check_output(["lfs", "quota", "-gq", groupName,
                                                 "/lustre/{}".format(volume)], encoding="UTF-8").split()[3]) * 1024
        except subprocess.CalledProcessError:
            # some groups don't have mercury as a member, which means their
            # quotas can't be checked and the above command throws an error
            quota = None

        archivedDirs = None
        # only check whether a volume is archived if it's smaller than 100MiB,
        # any larger than that and it's very likely to still be in use
        if (volumeSize < 100*1024**2):
            try:
                archivedDirs = scanDirectory(group_directories[volume][0] +
                                             groupName)
            except FileNotFoundError:
                pass

            # only test the next directory if .imirrored wasn't already found
            if archivedDirs is None:
                try:
                    archivedDirs = scanDirectory(group_directories[volume][1] +
                                                 groupName)
                except FileNotFoundError:
                    pass
        if (volumeSize == 0 and lastModified_unix == 0):
            # not a useful entry, ignore it
            pass
        else:
            group_data.append((gidNumber, groupName, PI, volumeSize, volume,
                              lastModified, quota, archivedDirs, isHumgen))

    print("Processed data for {}.".format(volume), flush=True)

    return (volume, group_data)


def generate_tables(tmp_db: sqlite3.Connection):
    # Create the temporary sqlite database tables
    db_cursor = tmp_db.cursor()
    for vol in VOLUMES:
        print(f"Creating table scratch{vol}")
        db_cursor.execute(f"""
            CREATE TABLE scratch{vol} (gidNumber INTEGER PRIMARY KEY,
            groupName TEXT, PI TEXT, volumeSize INTEGER, volume TEXT,
            lastModified INTEGER, quota INTEGER,
            archivedDirs TEXT, isHumgen INTEGER)
        """)
        tmp_db.commit()
    print("created tables")


def main() -> None:
    # temporary SQLite database used to organise data
    tmp_db = sqlite3.connect(DATABASE_NAME)

    print("Establishing MySQL connection...")
    sql_db = db.common.getSQLConnection(config)

    # Finding most recent mpistat files for each volume
    # We only care if the most recent mpistat file isn't already in the database
    mpistat_files: T.List[str] = []
    mpistat_dates: T.Dict[int, datetime.date] = {}

    for volume in VOLUMES:
        latest_mpi = utils.finder.findReport(f"scratch{volume}", MPISTAT_DIR)
        mpi_date_str = latest_mpi.split("/")[-1].split("_")[0]
        mpi_date = datetime.date(int(mpi_date_str[:4]), int(
            mpi_date_str[4:6]), int(mpi_date_str[6:8]))

        if not db.report.checkReportDate(sql_db, mpi_date, volume):
            mpistat_files.append(latest_mpi)
            mpistat_dates[volume] = mpi_date

    print("Establishing LDAP connection...")
    ldap_con = utils.ldap.getLDAPConnection()

    print("Collecting group information...")
    utils.ldap.add_humgen_ldap_to_db(ldap_con, tmp_db)

    # Create the tables in the temporary DB
    generate_tables(tmp_db)

    # creates a process pool which will concurrently execute 5 processes
    # to read each mpistat file
    pool = multiprocessing.Pool(processes=5)

    print("Starting mpistat processors...", flush=True)
    # sorts file list alphabetically, so that the volumes are in the same order
    # regardless of how the script arguments are given. This is useful for
    # having an ordered multiprocess output later.
    mpistat_files.sort()

    # distribute input files to processes running instances of processMpistat()
    try:
        mpi_data = pool.map(processMpistat, mpistat_files)
        pool.close()
        pool.join()
    except Exception as e:
        sql_db.close()
        tmp_db.close()
        os.remove(DATABASE_NAME)
        raise e

    tables = [x[0] for x in mpi_data]
    group_data = [x for y in mpi_data for x in y[1]]

    db_cursor = tmp_db.cursor()

    print("adding data to temporary database")
    for entry in group_data:
        (gidNumber, groupName, PI, volumeSize, volume, lastModified,
         quota, archivedDirs, isHumgen) = entry
        db_cursor.execute("""INSERT INTO {} (gidNumber, groupName, PI,
                    volumeSize, volume, lastModified, quota, archivedDirs, isHumgen)
                    VALUES (?,?,?,?,?,?,?,?,?)""".format(volume),
                          (gidNumber, groupName,
                           PI, volumeSize, volume, lastModified, quota,
                           archivedDirs, isHumgen)
                          )

    tmp_db.commit()
    print("added data to temporary database")

    date = datetime.date.today().strftime("%Y-%m-%d")

    print("Transferring report data to MySQL database...")
    db.report.load_usage_report_to_sql(tmp_db, sql_db, tables, mpistat_dates)

    print("Writing report data to .tsv file...")
    utils.tsv.createTsvReport(tmp_db, tables, date, REPORT_DIR)

    print("Cleaning up...")
    sql_db.close()
    tmp_db.close()
    # delete the on-disk SQLite database file
    os.remove(DATABASE_NAME)
    print("Done.")


if __name__ == "__main__":
    main()
