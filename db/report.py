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
    # 
    # Then, we can go over all the data from the tmp_db and put it into the main db

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
