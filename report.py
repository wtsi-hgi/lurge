import mysql.connector
import os
import re
import datetime
import csv
import subprocess
import multiprocessing
import sqlite3
import base64
import ldap
import gzip
import sys
# importing report_config.py file, not a library
import report_config as config

# This script should be executed by reportmanager.sh. If you want to run it
# directly, you need to execute it like so:
# python3 report.py [date] [filename list]
# [date] needs to be an ISO date string (ie, "2019-09-21")
# [filename list] needs to be a list of mpistat output files
# (ie, latest-119.dat.gz latest-118.dat.gz etc)

# global (gasp!!) variable used to set the filename of the SQLite database
# each process will access. Shouldn't really ever be changed, but it's here
# for the sake of convenience.
DATABASE_NAME = "_lurge_tmp_sqlite.db"

def checkReportDate(sql_db, date):
    """
    Checks the dates in the MySQL database, and stops the program if date 'date'
    is already recorded.

    :param sql_db: MySQL connection to check for reports
    :param date: The date of the report to be produced
    """
    sql_cursor = sql_db.cursor()
    sql_cursor.execute("""SELECT DISTINCT `date` FROM lustre_usage""")

    for result in sql_cursor:
        if (date == result):
            exit("Report for date {} already found in MySQL database! \
                Exiting.".format(date))

def getSQLConnection():
    # connects to the MySQL server used to store the report data, change the
    # credentials here to point at your desired database
    if (config.PORT == None):
        port = 3306 # if port is not set, set it to default
    else:
        port = config.PORT

    db_con = mysql.connector.connect(
        host=config.HOST,
        database=config.DATABASE,
        port = port,
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
    # cn = groupName, sangerProjectPI['uid'] = PI's user id
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

def scanDirectory(directory):
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

def processMpistat(mpi_file):
    """
    Processes a single mpistat output file and writes a table of results to
    SQLite. Intended to be ran multiple times concurrently for multiple files.

    :param mpi_file: File name of mpistat output file to process
    """
    global DATABASE_NAME
    tmp_db = sqlite3.connect(DATABASE_NAME)

    db_cursor = tmp_db.cursor()
    db_cursor.execute('''SELECT gidNumber, groupName, PI FROM group_table''')

    # reads first line of the mpistat file to establish the scratch volume
    with gzip.open(mpi_file, 'rt') as mpi_text:
        # each line is a whitespace separated list of fields, the first element
        # is the directory
        b64_directory = mpi_text.readline().split()[0]
        # b64_directory is a base64 encoded string '/lustre/scratch[XYZ]'
        directory = base64.b64decode(b64_directory).decode("UTF-8")
        # gets the last element of the directory, ie "scratch[XYZ]"
        volume = directory.split("/")[-1]

    groups = {}
    for row in db_cursor:
        # rearranging into a more usable format, since each row is in the
        # form (gid, "groupname", "PIname")
        gid, groupName, PIname = row

        # lastmodified is in Unix time and will be converted to "days since
        # last modification" later
        groups[str(gid)] = {'groupName':groupName, 'PIname':PIname, 'volumeSize':0,
            'lastModified':0, 'volume':volume, 'isHumgen':True}

    # lazily reads the mpistat file without unzipping the whole thing first
    starttime = datetime.datetime.now()
    lines_processed = 0
    print("Opening {} for reading...".format(mpi_file))
    # directories with group directories to scan for .imirrored
    group_directories = {
        'scratch114':["/lustre/scratch114/teams/", "/lustre/scratch114/project/"],
        'scratch115':["/lustre/scratch115/teams/", "/lustre/scratch115/projects/"],
        'scratch118':["/lustre/scratch118/humgen/old-team-data/",
            "/lustre/scratch118/humgen/hgi/projects/"],
        'scratch119':["/lustre/scratch119/humgen/teams",
            "/lustre/scratch119/humgen/projects/"]
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
                print("{} files processed for {}".format(lines_processed, volume))

            line = line.split()

            gid = line[3]
            try:
                groups[gid]['volumeSize'] += int(line[1])
                # only update the group's last edit time if it's more recent
                if (int(line[5]) > groups[gid]['lastModified']):
                    groups[gid]['lastModified'] = int(line[5])
            except KeyError:
                # the first time the group appears, add it to the dictionary
                # its name will be found later
                groups[gid] = {'groupName':None, 'PIname':None,
                    'volumeSize':int(line[1]), 'lastModified':int(line[5]),
                    'volume':volume, 'isHumgen':False}

            lines_processed += 1

    # creates new table named after the volume being analysed (ie, scratch114)
    db_cursor.execute('''CREATE TABLE {} (gidNumber INTEGER PRIMARY KEY,
        groupName TEXT, PI TEXT, volumeSize INTEGER, volume TEXT,
        lastModified INTEGER, quota INTEGER, consumption TEXT,
        archivedDirs TEXT, isHumgen INTEGER)'''.format(volume))

    # gets the Unix timestamp of when the mpistat file was created
    # int() truncates away the sub-second measurements
    mpistat_date_unix = int(os.stat(mpi_file).st_mtime)
    ldap_con = getLDAPConnection()
    for gid in groups:
        gidNumber = gid
        groupName = groups[gid]['groupName']

        # updates the group names for groups discovered during mpistat crawl
        if (groupName == None):
            result = ldap_con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
                ldap.SCOPE_ONELEVEL, "(gidNumber={})".format(gid), ["cn"])
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
        lastModified = round((mpistat_date_unix - lastModified_unix)/86400 , 1)

        if(lastModified < 0):
            lastModified = 0

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

        TEBI = 1024**4 # bytes in a tebibyte
        if quota is not None:
            try:
                consumption = "{} TiB of {} TiB ({}%)".format(
                    round(volumeSize/TEBI, 1), round(quota/TEBI, 1),
                    round(volumeSize/quota * 100, 1))
            except ZeroDivisionError:
                # this happens sometimes, when quota is 0
                consumption = "{} TiB of 0 bytes (Inf%)".format(
                    round(volumeSize/TEBI, 1))
        else:
            consumption = "{} TiB".format(round(volumeSize/TEBI, 1))

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
            db_cursor.execute('''INSERT INTO {}(gidNumber, groupName, PI,
                volumeSize, volume, lastModified, quota, consumption, archivedDirs, isHumgen)
                VALUES (?,?,?,?,?,?,?,?,?,?)'''.format(volume), (gidNumber, groupName,
                PI, volumeSize, volume, lastModified, quota, consumption,
                archivedDirs, isHumgen))

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
            consumption, lastModified, archivedDirs, isHumgen FROM {}
            ORDER BY volume ASC, PI ASC, groupName ASC'''.format(table))
        for row in tmp_cursor:
            # row elements are ordered like the column names in the select,
            # ie 'volume' is always row[0]
            instruction = '''INSERT INTO lustre_usage (`Lustre Volume`,
                `PI`, `Unix Group`, `Used (bytes)`, `Quota (bytes)`, `Consumption`,
                `Last Modified (days)`, `Archived Directories`, `IsHumgen`, `date`)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
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
            "Last Modified (days)", "Archived Directories", "Is Humgen?"])

        for table in tables:
            print("Inserting data for {}...".format(table))
            db_cursor.execute('''SELECT volume, PI, groupName, volumeSize,
                quota, consumption, lastModified, archivedDirs, isHumgen FROM {}
                ORDER BY volume ASC, PI ASC, groupName ASC'''.format(table))
            for row in db_cursor:
                # row elements are ordered like the column names in the select
                # statement

                # replace elements with no value with "-"
                data = ["-" if x==None else x for x in row]
                # replace SQLite 1/0 Booleans with True/False
                if data[-1] == 0:
                    data[-1] = False
                else:
                    data[-1] = True

                report_writer.writerow(data)

    print("{} created.".format(name))

if __name__ == "__main__":
    # ignore first argument (the name of the script)
    # second argument is the date, all other arguments are file names
    date = sys.argv[1]
    mpistat_files = sys.argv[2:]

    # checks if 'date' is formatted correctly
    if re.search("\d\d\d\d-\d\d-\d\d", date) is None:
        exit("Date formatting invalid, YYYY-MM-DD expected! Exiting.")

    # temporary SQLite database used to organise data
    tmp_db = sqlite3.connect(DATABASE_NAME)
    print("Establishing MySQL connection...")
    sql_db = getSQLConnection()

    checkReportDate(sql_db, date)

    print("Establishing LDAP connection...")
    ldap_con = getLDAPConnection()

    print("Collecting group information...")
    findGroups(ldap_con, tmp_db)

    # creates a process pool which will concurrently execute 4 processes
    # NOTE: If resources permit, change this to the number of mpistat files
    # that are going to be processed
    pool = multiprocessing.Pool(processes=4)

    print("Starting mpistat processors...")
    # sorts file list alphabetically, so that the volumes are in the same order
    # regardless of how the script arguments are given. This is useful for
    # having an ordered multiprocess output later.
    mpistat_files.sort()

    # distribute input files to processes running instances of processMpistat()
    tables = pool.map(processMpistat, mpistat_files)
    pool.close()
    pool.join()

    # finds the last modified date of some mpistat file
    date_unix = datetime.datetime.utcfromtimestamp( int(os.stat(
        mpistat_files[0]).st_mtime) )
    # converts datetime object into ISO date string
    date = "{0:%Y-%m-%d}".format(date_unix)

    # transfer content of SQLite tables into one MySQL table
    print("Transferring report data to MySQL database...")
    loadIntoMySQL(tmp_db, sql_db, tables, date)

    print("Writing report data to .tsv file...")
    createTsvReport(tmp_db, tables, date)

    print("Cleaning up...")
    sql_db.close()
    tmp_db.close()
    # delete the on-disk SQLite database file
    os.remove(DATABASE_NAME)
    print("Done.")
