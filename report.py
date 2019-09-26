import mysql.connector
import os
import re
import datetime
import csv
import subprocess
import multiprocessing
import sqlite3
import base
import ldap
import gzip
import sys
# importing report_config.py file, not a library
import report_config as config


def getSQLConnection():
    # connects to the MySQL server used to store the report data, change the
    # credentials here to point at your desired database
    db_con = mysql.connector.connect(
        host=config.HOST,
        database=config.DATABASE,
        user=config.USER,
        passwd=config.PASSWORD
    )

    return db_con

def getLDAPConnection():
    con = ldap.initialize("ldap://ldap-ro.internal.sanger.ac.uk:389")
    # Sanger internal LDAP is public so no credentials needed
    con.bind("","")

    return con

def findGroups(ldap_con, tmp_db):
    """
    Finds Humgen groups using LDAP and writes the group ID number, group name,
    and corresponding PI's surname to table 'group_table' in tmp_db.

    :param ldap_con: LDAP connection to use for queries
    :param tmp_db: SQLite database connection object to write 'group_table' to
    """

    results = ldap_con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
        ldap.SCOPE_ONELEVEL, "(objectClass=sangerHumgenProjectGroup)",
        ['cn', 'gidNumber', 'sangerProjectPI'])

    db_cursor = tmp_db.cursor()
    # cn = groupName, sangerProjectPI['uid'] = PI user id
    db_cursor.execute('''CREATE TABLE group_table(gidNumber INTEGER PRIMARY KEY,
        groupName TEXT, PI TEXT)''')
    tmp_db.commit()

    # used to collect every PI uid that needs to be resolved, use of set means
    # that duplicate uids are ignored
    PIuids = set()

    for item in results:
        # gidNumber is stored as a byte-encoded string in results
        gidNumber = int( item[1]['gidNumber'][0].decode('UTF-8') )
        groupName = item[1]['cn'][0].decode('UTF-8')

        # not all groups have a PI, KeyError thrown in those cases
        try:
            # string in the form 'uid=[xyz],ou=people,dc=sanger,dc=ac,dc=uk'
            # we want just [xyz]
            PIdn = item[1]['sangerProjectPI'][0].decode('UTF-8')

            # first split = 'uid=[xyz]'
            # second split = '[xyz]'
            PIuid = PIdn.split(',')[0].split('=')[1]
            PIuids.add( PIuid )
        except KeyError:
            # nothing to do except skip the PI related code
            PIuid = None

        db_cursor.execute('''INSERT INTO group_table(gidNumber, groupName, PI)
            VALUES(?,?,?)''', (gidNumber, groupName, PIuid))

    # write all prior INSERTs to table in one transaction
    tmp_db.commit()

    # replaces uids in group_table with full surnames of the corresponding PI
    for uid in PIuids:
        surname_result = ldap_con.search_s("ou=people,dc=sanger,dc=ac,dc=uk",
            ldap.SCOPE_ONELEVEL, "(uid={})".format(uid), ['sn'])

        surname = surname_result[0][1]['sn'][0].decode('UTF-8')

        db_cursor.execute('''UPDATE group_table SET PI = ? WHERE PI = ?''',
            (surname, uid))

    # write prior UPDATEs to table, nothing needs returning
    tmp_db.commit()
    print("Created table of Humgen Unix groups.")

def processMpistat(tmp_db, mpi_file):
    """
    Processes a single mpistat output file and writes a table of results to
    tmp_db. Intended to be ran multiple times concurrently for multiple files.

    :param tmp_db: Database to write table of results to
    :param mpi_file: File name of mpistat output file to process, named
        'latest-[xyz].dat.gz'
    """
    # gets rid of file extensions
    file_name = mpi_file.split(".")[0]
    # gets the last 3 characters of the file name, which should be the scratch
    # volume number
    volume = "scratch" + file_name[-3:]

    db_cursor = tmp_db.cursor()
    db_cursor.execute('''SELECT gidNumber, groupName, PI FROM group_table''')

    groups = {}
    for row in cursor:
        # rearranging into a more usable format, since each row is in the
        # form (gid, "groupname", "PIname")
        gid, groupname, PIname = row

        # lastmodified is in Unix time and will be converted to "days since
        # last modification" later
        groups[gid] = {'groupName':groupName, 'PIname':PIname, 'volumeSize':0,
            'lastModified':0, 'volume':volume}

    # lazily reads the mpistat file without unzipping the whole thing first
    starttime = datetime.datetime.now()
    lines_processed = 0
    print("Opening {} for reading...".format(mpi_file))
    with gzip.open(mpi_file, 'rt') as mpi_text:
        # each line in the mpistat file has the following whitespace separated
        # fields:
        # base64 encoded path, file size, owner uid, owner gid, last access time,
        # last modification time, last status change time, object type (file,
        # directory or link), inode number, number of links, device id
        for line in mpi_text:

            # print out progress report every ~60 seconds
            if (datetime.datetime.now() - starttime).seconds > 60:
                starttime = datetime.datetime.now()
                print("{} files processed for {}".format(lines_processed, volume))

            line = line.split()

            gid = line[3]
            try:
                groups[gid]['volumeSize'] += line[1]
                # only update the group's last edit time if it's more recent
                if (line[5] > groups[gid]['lastModified']):
                    groups[gid]['lastModified'] = line[5]
            except KeyError:
                # skip the line if the group wasn't found during the LDAP search
                pass

            lines_processed += 1

    # creates new table named after the volume being analysed (ie, scratch114)
    db_cursor.execute('''CREATE TABLE {} (gidNumber INTEGER PRIMARY KEY,
        groupName TEXT, PI TEXT, volumeSize INTEGER, volume TEXT,
        lastModified INTEGER, quota INTEGER, consumption TEXT,
        archivedDirs TEXT)'''.format(volume))

    # gets the Unix timestamp of when the mpistat file was created
    # int() truncates away the sub-second measurements
    mpistat_date_unix = int(os.stat(mpi_file).st_mtime)

    for gid in groups:
        gidNumber = gid
        groupName = groups[gid]['groupName']
        PI = groups[gid]['PIname']
        volumeSize = groups[gid]['volumeSize']
        lastModified_unix = groups[gid]['lastModified']
        # mpistat_date_unix - lastModified_unix is the seconds since last
        # modification relative to when the mpistat file was produced
        # divided by 86400 (seconds in a day) to find day difference
        lastModified = int( (mpistat_date_unix - lastModified_unix)/86400 )

        # lfs quota query is split into a list based on whitespace, and the
        # fourth element is taken as the quota. it's in kilobytes though, so it
        # is multiplied by 1024
        try:
            quota = subprocess.check_output(["lfs", "quota", "-gq", groupName,
                "/lustre/{}".format(volume)], encoding="UTF-8").split()[3] * 1024
        except subprocess.CalledProcessError:
            # some groups don't have mercury as a member, which means their
            # quotas can't be checked and the above command throws an error
            quota = None

        TEBI = 1024**4 # bytes in a tebibyte
        if quota is not None:
            consumption = "{} TiB of {} TiB ({}%)".format(
                round(volumeSize/TEBI, 1), round(quota/TEBI, 1),
                round(volumeSize/quota * 100, 1))
        else:
            consumption = "{} TiB".format(round(volumeSize/TEBI, 1))

        # only check whether a volume is archived if it's smaller than 100MiB,
        # any larger than that and it's very likely to still be in use
        archivedDirs = None
        if (volumeSize > 100*1024**2):
            with os.scandir("/lustre/{}/humgen/projects/{}".format(volume,
                groupName)) as items:
                # scan the project directory for the ".imirrored" file
                if item.name == ".imirrored":
                    archivedDirs = "/lustre/{}/humgen/projects/{}".format(
                        volume, groupName)

        # same volume that was passed to the function is used
        db_cursor.execute('''INSERT INTO {}(gidNumber, groupName, PI,
            volumeSize, volume, lastModified, quota, consumption, archivedDirs)
            VALUES (?,?,?,?,?,?,?,?,?)'''.format(volume), (gidNumber, groupName,
            PI, volumeSize, volume, lastModified, quota, consumption,
            archivedDirs))

    tmp_db.commit()
    print("Processed data for {}.".format(volume))

    return volume

def loadIntoMySQL(tmp_db, sql_db, tables, date):
    """
    Reads the contents of tables in tmp_db and writes them to a MySQL database.

    :param tmp_db: SQLite database in which tables are stored
    :param sql_db: MySQL database into which to write data
    :param tables: List of table names to read
    :param date: Date string to label the data (ie, "2019-09-20")
    """
    tmp_cursor = tmp_db.cursor()
    sql_cursor = sql_db.cursor()

    # iterates over each row in each SQLite table, and just moves the data over
    # into a single MySQL table
    for table in tables:
        print("Inserting data for {}...".format(table))
        tmp_cursor.execute('''SELECT volume, PI, groupName, volumeSize, quota,
            consumption, lastModified, archivedDirs FROM {}
            ORDER BY volume ASC, PI ASC, groupName ASC'''.format(table))
        for row in tmp_cursor:
            # row elements are ordered like the column names in the select,
            # ie 'volume' is always row[0]
            instruction = '''INSERT INTO lustre_usage (`Lustre Volume`,
                `PI`, `Unix Group`, `Used (bytes)`, `Quota (bytes)`, `Consumption`,
                `Last Modified (days)`, `Archived Directories`, `date`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            # creates single data tuple from existing row tuple and singleton
            # date tuple, plain variables can't be concatenated to tuples
            data = row + (date,)

            sql_cursor.execute(instruction, data)

    sql_db.commit()
    print("Report data for {} loaded into MySQL.".format(date))

def createTsvReport(tmp_db, tables, date):
    """
    Reads the contents of tables in tmp_db and writes them to a .tsv formatted
    file.

    :param tmp_db: SQLite database from which to fetch data
    :param tables: List of table names to read
    :param date: Date string of the data to be used (ie, "2019-09-20")
    """
    # sets filename to 'report-YYYYMMDD.tsv'
    name = "report-{}.tsv".format(date.replace("-", ""))
    db_cursor = tmp_db.cursor()

    with open(name, "w", newline="") as reportfile:
        # start a writer that will format the file as tab-separated
        report_writer = csv.writer(reportfile, delimiter="\t",
            quoting = csv.QUOTE_NONE)
        # write column headers
        report_writer.writerow(["Lustre Volume", "PI", "Unix Group",
            "Used (bytes)", "Quota (bytes)", "Consumption",
            "Last Modified (days)", "Archived Directories"])

        for table in tables:
            print("Inserting data for {}...".format(table))
            db_cursor.execute('''SELECT volume, PI, groupName, volumeSize,
                quota, consumption, lastModified, archivedDirs FROM {}
                ORDER BY volume ASC, PI ASC, groupName ASC'''.format(table))
            for row in db_cursor:
                # row elements are ordered like the column names in the select
                report_writer.writerow([row[0], row[1], row[2], row[3], row[4],
                    row[5], row[6], row[7]])

    print("{} complete.".format(name))

if __name__ == "__main__":
    # ignore first argument (the name of the script)
    # second argument is the date, all other arguments are file names
    date = sys.argv[1]
    mpistat_files = sys.argv[2:]
    # temporary in-memory SQLite database used to organise data
    print("Establishing LDAP connection...")
    tmp_db = sqlite3.connect(':memory:')

    ldap_con = getLDAPConnection()

    print("Collecting group information...")
    findGroups(ldap_con, tmp_db)

    # creates a process pool which will concurrently execute 4 processes
    # NOTE: If resources permit, change this to the number of mpistat files
    # that are going to be processed
    pool = multiprocessing.Pool(processes=4)

    print("Starting mpistat processors...")
    # sorts file list alphabetically, so that the volumes are in the same order
    # regardless of how the script is called. this is useful for having an
    # ordered multiprocess output later.
    mpistat_files.sort()
    # creates list of pairs, [tmp_db, "latest-YYYYMMDD.dat.gz"]
    work = [[tmp_db, mpi_file] for mpi_file in mpistat_files]

    # distribute input pairs to processes running instances of processMpistat()
    tables = pool.map(processMpistat, work)

    # finds the last modified date of the mpistat file
    date_unix = datetime.datetime.utcfromtimestamp( int(os.stat(
        mpistat_files[0].st_mtime)) )
    # converts datetime object into ISO date string
    date = "{0:%Y-%m-%d}".format(date_unix)
    # transfer content of separate SQLite tables into one MySQL table
    print("Establishing MySQL connection...")
    sql_db = getSQLConnection())
    print("Transferring report data to MySQL database...")
    loadIntoMySQL(tmp_db, sql_db, tables, date)

    print("Writing report data to .tsv file...")
    createTsvReport(tmp_db, tables, date)

    sql_con.close()
    tmp_db.close()
