import csv


def createTsvReport(tmp_db, tables, date, report_dir):
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

    with open(report_dir+"report-output-files/"+name, "w", newline="") as reportfile:
        # start a writer that will format the file as tab-separated
        report_writer = csv.writer(reportfile, delimiter="\t",
                                   quoting=csv.QUOTE_NONE)
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
                data = ["-" if x == None else x for x in row]
                # replace SQLite 1/0 Booleans with True/False
                if data[-1] == 0:
                    data[-1] = False
                else:
                    data[-1] = True

                report_writer.writerow(data)

    print("{} created.".format(name))
