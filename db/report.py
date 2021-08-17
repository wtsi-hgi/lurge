import os
from collections import defaultdict

import mysql.connector


def getSQLConnection(config):
    # connects to the MySQL server used to store the report data, change the
    # credentials here to point at your desired database
    port = config.PORT if config.PORT is None else 3306

    db_con = mysql.connector.connect(
        host=config.HOST,
        database=config.DATABASE,
        port=port,
        user=config.USER,
        passwd=config.PASSWORD
    )

    return db_con


def checkReportDate(sql_db, date, db_name):
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
            os.remove(db_name)
            raise FileExistsError("Report for date {} already found in MySQL database! \
                Exiting.".format(date))


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

    # First, get foreign keys from the MySQL database for PI, Volume and Unix Group
    # We'll also have to add any that don't exist

    # PI
    sql_cursor.execute("SELECT * FROM hgi_lustre_usage_new.pi")
    pi_results = sql_cursor.fetchall()

    pis = {}
    for (pi_id, pi_name) in pi_results:
        pis[pi_name] = pi_id

    # Unixgroups
    sql_cursor.execute("SELECT * FROM hgi_lustre_usage_new.unix_group")
    group_results = sql_cursor.fetchall()

    groups = defaultdict(__dict__)
    for (group_id, group_name, isHumgen) in group_results:    
        groups[group_name][isHumgen] == group_id


    # Volumes
    sql_cursor.execute("SELECT * FROM hgi_lustre_usage_new.volume")
    volume_results = sql_cursor.fetchall()

    volumes = {}
    for (volume_id, volume_name) in volume_results:
        volumes[volume_name] = volume_id


    # Then, we can go over all the data from the tmp_db and put it into the main db

    # iterates over each row in each SQLite table, and just moves the data over
    # into a single MySQL table
    for table in tables:
        print("Inserting data for {}...".format(table))
        tmp_cursor.execute('''SELECT volume, PI, groupName, volumeSize, quota,
            consumption, lastModified, archivedDirs, isHumgen FROM {}
            ORDER BY volume ASC, PI ASC, groupName ASC'''.format(table))
        for (volume, pi_name, group, size, quota, last_mod, archived, isHumgen) in tmp_cursor:

            # Making sure the PI, Group and Volume all exist in the DB
            if pi_name is not None:
                try:
                    pi = pis[pi_name]
                except KeyError:
                    sql_cursor.execute("INSERT INTO hgi_lustre_usage_new.pi (pi_name) VALUES (?);", pi_name)
                    sql_cursor.execute("SELECT pi_id FROM hgi_lustre_usage_new.pi WHERE pi_name = %s", pi_name)
                    (pi) = sql_cursor.fetchone()
                    pis[pi_name] = pi
            else:
                pi = None

            if group not in groups or isHumgen not in groups[group]:
                sql_cursor.execute("INSERT INTO hgi_lustre_usage_new.unix_group (group_name, is_humgen) VALUES (%s, %s);", (group, isHumgen))
                sql_cursor.execute("SELECT group_id FROM hgi_lustre_usage_new.unix_group WHERE group_name = %s AND is_humgen = %s;", (group, isHumgen))
                (group_id) = sql_cursor.fetchone()
                groups[group_name][isHumgen] = group_id
            
            if volume not in volumes:
                sql_cursor.execute("INSERT INTO hgi_lustre_usage_new.volume (scratch_disk) VALUES (%s);", volume)
                sql_cursor.execute("SELECT volume_id FROM hgi_lustre_usage_new.volume WHERE scratch_disk = %s;", volume)
                (volume_id) = sql_cursor.fetchone()
                volumes[volume] = volume_id

            # Add our data
            query = """INSERT INTO hgi_lustre_usage_new.lustre_usage (used, quota, record_date, archived,
                last_modified, pi_id, unix_id, volume_id) VALUES (%d, %d, %s, %s, %d, %d, %d, %d);"""

            sql_cursor.execute(query, (
                size, 
                quota, 
                date, 
                archived is not None,
                last_mod, 
                pi,
                groups[group][isHumgen],
                volumes[volume]
            ))

    sql_db.commit()
    print("Report data for {} loaded into MySQL.".format(date))