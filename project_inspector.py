#!/usr/bin/env python3

import gzip
import base64
import re
import argparse
import pathlib
import datetime
import sys
import os
import multiprocessing

from itertools import repeat

import ldap
import mysql.connector

# report_config.py is a SQL credentials file, not a library
import report_config as config

# TODO: find more sophisticated way to detect bam/cram/vcf files
bam = re.compile("\.(bam|sam)(\.gz)?$")
cram = re.compile("\.cram(\.gz)?$")
vcf = re.compile("\.(vcf|bcf|gvcf)(\.gz)?$")
pedbed = re.compile("\.(ped|bed)(\.gz)?$")

REPORT_DIR = "/lustre/scratch114/teams/hgi/lustre_reports/mpistat/data/"
# put any link mappings here, the program only scans mpistat filenames so it can't
# resolve symbolic links
PROJECT_DIRS = {
    'lustre/scratch115/projects': 'lustre/scratch115/realdata/mdt[0-9]/projects',
    'lustre/scratch119/humgen/projects': 'lustre/scratch119/realdata/mdt[0-9]/projects',
    'lustre/scratch115/teams': 'lustre/scratch115/realdata/mdt[0-9]/teams',
    'lustre/scratch119/humgen/teams': 'lustre/scratch119/realdata/mdt[0-9]/teams'
}

ALL_PROJECTS = {
    '114': ["lustre/scratch114/projects", "lustre/scratch114/teams"],
    '115': ["lustre/scratch115/realdata/mdt[0-9]/projects", "lustre/scratch115/realdata/mdt[0-9]/teams"],
    '118': ["lustre/scratch118/humgen/hgi/projects", "lustre/scratch118/humgen/old-team-data"],
    '119': ["lustre/scratch119/realdata/mdt[0-9]/projects", "lustre/scratch119/realdata/mdt[0-9]/teams"]
}

parser = argparse.ArgumentParser(description="Creates a tab-separated table summarising disk usage of a project directory, and the total size of BAM/CRAM/VCF/BED/PED files inside.")

parser.add_argument('--depth', '-d', nargs='?', type=int, default=2,
    help="The depth of the output. A depth of 0 shows the summary for the root path only, a depth of 1 shows the summary for each subdirectory, and a depth of 2 also shows summaries for the children of the subdirectories. Set to 2 by default.")

parser.add_argument('--generic', dest="mode", action="store_const", const="general", default="project",
    help="Make the output generic. When this flag is used, the output will have fewer redundant columns, and the Project/Directory column will be replaced by a single Path. Use when scanning paths that aren't project directories.")

parser.add_argument('--noheader', dest="header", action="store_const", const=False, default=True,
    help="Don't print the column header.")

parser.add_argument('--tosql', dest="tosql", action="store_const",
    const=True, default=False,
    help="In addition to printing the result to stdout, this flag will make the program write the output directly to a MySQL database.")

parser.add_argument('path', nargs='?',
    help="The path to scan. The final directory in the path is considered the root. Leave empty to scan HGI project directories on different volumes all at the same time.")

def humanise(number):
    """Converts bytes to human-readable string."""
    if number/2**10 < 1:
        return "{}".format(number)
    elif number/2**20 < 1:
        return "{} KiB".format(round(number/2**10, 2))
    elif number/2**30 < 1:
        return "{} MiB".format(round(number/2**20, 2))
    elif number/2**40 < 1:
        return "{} GiB".format(round(number/2**30, 2))
    elif number/2**50 < 1:
        return "{} TiB".format(round(number/2**40, 2))
    else:
        return "{} PiB".format(round(number/2**50, 2))

def getParents(dir):
    """Returns list of directories which are a parent to 'dir'"""
    split_dir = dir.split("/")
    parents = []

    for i in range(1, len(split_dir)):
        parents.append("/".join(split_dir[0:i]))

    return parents

def findReport(dir):
    """Finds most recent mpistat output relevant to 'dir'"""
    report_date = datetime.date.today()
    # NOTE: this assumes dir is always /lustre/scratchXYZ, which it should be
    # unless infrastructure changes
    volume = dir[-3:]
    success = False
    filename = ""

    while success is False:
        filename = "{}_{}.dat.gz".format(report_date.strftime("%Y%m%d"), volume)
        try:
            gzip.open(REPORT_DIR+filename, 'rt')
            success = True
        except FileNotFoundError:
            success = False
            report_date -= datetime.timedelta(days=1)

    if report_date != datetime.date.today():
        print("Warning, couldn't find mpistat output for today. Used mpistat output for {0:%Y-%m-%d} instead.".format(report_date), file=sys.stderr)

    return REPORT_DIR+filename

def getHumgenNames():
    con = ldap.initialize("ldap://ldap-ro.internal.sanger.ac.uk:389")
    con.bind('','')

    results = con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
        ldap.SCOPE_ONELEVEL, "(objectClass=sangerHumgenProjectGroup)",
        ['gidNumber', 'sangerProjectPI', 'cn'])

    groups_pis = {}
    groups_names = {}

    for entry in results:
        gid = entry[1]['gidNumber'][0].decode("UTF-8", "replace")
        PIuid = entry[1]['sangerProjectPI'][0].decode("UTF-8", "replace")
        group_name = entry[1]['cn'][0].decode("UTF-8", "replace")
        groups_pis[gid] = PIuid
        groups_names[gid] = group_name

    PIs = set(groups_pis.values())
    uid_sn = {}

    for PIuid in PIs:
        _uid = PIuid.split(',')[0].split('=')[1]
        result = con.search_s("ou=people,dc=sanger,dc=ac,dc=uk",
            ldap.SCOPE_ONELEVEL, "(uid={})".format(_uid), ['sn'])

        surname = result[0][1]['sn'][0].decode("UTF-8")
        uid_sn[PIuid] = surname

    for gid in groups_pis:
        surname = uid_sn[groups_pis[gid]]
        groups_pis[gid] = surname

    return (groups_pis, groups_names)

def createMapping(path, names, depth):
    """
    Returns a dictionary mapping paths to a dictionary of properties.

    @param path Path to the root directory which the program will scan for
    @param names Dictionary mapping group IDs to the group name and
        surname of their PI
    @param depth How many levels below the root to scan
    """
    FULL_PATH = path
    HUMGEN_PIS, HUMGEN_GROUPS = names
    DEPTH = depth

    # the scratch and root_parent values are the same for every project/team
    # directory pair
    if type(path) == list:
        segmented_path = FULL_PATH[0].split("/")
    else:
        segmented_path = FULL_PATH.split("/")
        # the single path string is put in a list for the sake of convenience
        FULL_PATH = [FULL_PATH]

    # assuming we start with /lustre/scratch114/projects, we get
    # /lustre/scratch114
    scratch = "/".join(segmented_path[0:2])
    # /lustre/scratch114/
    root_parent = "/".join(segmented_path[0:-1])

    report_path = findReport(scratch)
    # used to check last modified time against time mpistat output ran
    MPI_DATE = int(os.stat(report_path).st_mtime)

    dir_dict = {}

    lines = 0
    print("Reading mpistat output for {}".format(scratch), file=sys.stderr)
    with gzip.open(report_path, 'rt') as mpistat:
        for line in mpistat:
            lines += 1
            if lines % 500000 == 0:
                print("{} lines read (volume {})".format(lines, scratch),
                    file=sys.stderr)
            line = line.split()

            entry_path = base64.b64decode(line[0]).decode("UTF-8", "replace").strip("/")

            # if the entry path doesn't contain any of the target directories,
            # it's skipped and the next entry is read
            skip = True
            for path in FULL_PATH:
                if re.match(path, entry_path) is not None:
                    skip = False

            if skip:
                continue

            # removes everything above the root directory from the path
            short_path = re.sub(root_parent, '', entry_path).strip("/")

            if line[7] == "d":
                _dir = short_path.split("/")[0:-1]

                try:
                    if _dir[2].lower() == "users":
                        dir = "/".join(short_path.split("/")[0:DEPTH+1])
                    else:
                        dir = "/".join(short_path.split("/")[0:DEPTH])
                except IndexError:
                    dir = "/".join(short_path.split("/")[0:DEPTH])

                if line[3] in HUMGEN_PIS.keys():
                    pi = HUMGEN_PIS[line[3]]
                else:
                    pi = "-"

                if line[3] in HUMGEN_GROUPS.keys():
                    group_name = HUMGEN_GROUPS[line[3]]
                else:
                    group_name = "-"

                if dir not in dir_dict.keys():
                    dir_dict[dir] = {"total": 0, "bam": 0, "cram": 0, "vcf": 0, "pedbed": 0, "files": 1, "mtime": int(line[5]), "pi": pi, "group_name": group_name, "volume": scratch[-3:]}

                    for parent in getParents(dir):
                        if parent not in dir_dict.keys():
                            dir_dict[parent] = {"total": 0, "bam": 0, "cram": 0, "vcf": 0, "pedbed": 0, "files": 1, "mtime": int(line[5]), "pi": "-", "group_name": group_name, "volume": scratch[-3:]}

                dir_dict[dir]["pi"] = pi
                dir_dict[dir]["group_name"] = group_name

            elif line[7] == "f":
                _dir = short_path.split("/")[0:-1]

                try:
                    # hack to go one level deeper if there is a users/ folder
                    # in the project directory (ie, projects/xyz/users/)
                    if _dir[2].lower() == "users":
                        dir = "/".join(short_path.split("/")[0:-1][0:DEPTH+1])
                    else:
                        dir = "/".join(short_path.split("/")[0:-1][0:DEPTH])
                except IndexError:
                    # removes the last element of the path to prevent
                    # individual files from getting entries in the report
                    dir = "/".join(short_path.split("/")[0:-1][0:DEPTH])

                size = int(line[1])
                mtime = int(line[5])
                if line[3] in HUMGEN_PIS.keys():
                    pi = HUMGEN_PIS[line[3]]
                else:
                    pi = "-"

                if line[3] in HUMGEN_GROUPS.keys():
                    group_name = HUMGEN_GROUPS[line[3]]
                else:
                    group_name = "-"

                # if the directory hasn't been added to the dictionary yet, it
                # and all its parents are created
                if dir not in dir_dict.keys():
                    dir_dict[dir] = {"total": 0, "bam": 0, "cram": 0, "vcf": 0, "pedbed": 0, "files": 0, "mtime": mtime, "pi": pi, "group_name": group_name, "volume": scratch[-3:]}

                    for parent in getParents(dir):
                        if parent not in dir_dict.keys():
                            dir_dict[parent] = {"total": 0, "bam": 0, "cram": 0, "vcf": 0, "pedbed": 0, "files": 0, "mtime": mtime, "pi": "-", "group_name": group_name, "volume": scratch[-3:]}

                # TODO: abstract this away into something neater

                # updates values for the directory and all its parents
                dir_dict[dir]["total"] += size
                dir_dict[dir]["files"] += 1
                dir_dict[dir]["pi"] = pi
                dir_dict[dir]["group_name"] = group_name

                if mtime > dir_dict[dir]["mtime"]:
                    dir_dict[dir]["mtime"] = mtime
                for parent in getParents(dir):
                    dir_dict[parent]["total"] += size
                    dir_dict[parent]["files"] += 1
                    if mtime > dir_dict[parent]["mtime"]:
                        dir_dict[parent]["mtime"] = mtime

                if bam.search(short_path):
                    dir_dict[dir]["bam"] += size
                    for parent in getParents(dir):
                        dir_dict[parent]["bam"] += size

                elif cram.search(short_path):
                    dir_dict[dir]["cram"] += size
                    for parent in getParents(dir):
                        dir_dict[parent]["cram"] += size

                elif vcf.search(short_path):
                    dir_dict[dir]["vcf"] += size
                    for parent in getParents(dir):
                        dir_dict[parent]["vcf"] += size

                elif pedbed.search(short_path):
                    dir_dict[dir]["pedbed"] += size
                    for parent in getParents(dir):
                        dir_dict[parent]["pedbed"] += size

    return dir_dict

def printTable(dir_dict, scratch, mode):
    report_path = findReport(scratch)
    # used to check last modified time against time mpistat output ran
    MPI_DATE = int(os.stat(report_path).st_mtime)

    paths = list(dir_dict.keys())
    paths.sort()
    for key in paths:
        try:
            _project = key.split("/")[:2]
            _project = "/".join(_project)
        except IndexError:
            _project = "*TOTAL*"

        _path = "/".join(key.split("/")[2:])
        if _path == "":
            _path = "*TOTAL*"

        _files = dir_dict[key]["files"]
        # 86400 seconds in a day
        _mtime = round((MPI_DATE - dir_dict[key]["mtime"])/86400, 1)

        if(mode == "project"):
            _total = round(dir_dict[key]["total"] / 2**40, 3)
            _bam = round(dir_dict[key]["bam"] / 2**40, 3)
            _cram = round(dir_dict[key]["cram"] / 2**40, 3)
            _vcf = round(dir_dict[key]["vcf"] / 2**40, 3)
            _pedbed = round(dir_dict[key]["pedbed"] / 2**40, 3)
            _unix_group = dir_dict[key]["group_name"]

            _pi = dir_dict[key]["pi"]

            _volume = scratch[-3:]

            if _project[-1] != "/":
                # gets the total for the entire project
                _parenttotal = round(
                    dir_dict["/".join(key.split("/")[0:2])]["total"] / 2**40, 3)
            else:
                _parenttotal = _total

            print("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(_project, _path, _total, _bam, _cram, _vcf, _pedbed, _files, _mtime, _pi, _unix_group, _volume, _parenttotal))
        elif(mode == "general"):
            _total = humanise(dir_dict[key]["total"])
            _rawtotal = dir_dict[key]["total"]
            _bam = humanise(dir_dict[key]["bam"])
            _cram = humanise(dir_dict[key]["cram"])
            _vcf = humanise(dir_dict[key]["vcf"])
            _pedbed = humanise(dir_dict[key]["pedbed"])

            print("{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(key, _total, _bam, _cram, _vcf, _pedbed, _files, _mtime, _rawtotal))

def updateSQL(dict_of_dir_dicts, scratch):
    print("Writing result to spaceman database...", file=sys.stderr)
    if (config.PORT == None):
        port = 3306 # if the port isn't set in the config, it's the SQL default
    else:
        port = config.PORT

    db_con = mysql.connector.connect(
        host=config.HOST,
        database=config.DATABASE,
        port=port,
        user=config.USER,
        passwd=config.PASSWORD
    )

    cursor = db_con.cursor()

    # read/write user doesn't have temporary table permissions, so a persistent
    # table is used as a temporary table instead
    cursor.execute('''DELETE FROM spaceman_tmp''')

    report_path = findReport(scratch)
    MPI_DATE = int(os.stat(report_path).st_mtime)

    for dir_dict in dict_of_dir_dicts.values():
        paths = list(dir_dict.keys())
        paths.sort()

        for key in paths:
            try:
                _project = key.split("/")[:2]
                _project = "/".join(_project)
            except IndexError:
                _project = "*TOTAL*"

            _path = "/".join(key.split("/")[2:])
            if _path == "":
                _path = "*TOTAL*"

            _files = dir_dict[key]["files"]
            _mtime = round((MPI_DATE - dir_dict[key]["mtime"])/86400, 1)
            _total = round(dir_dict[key]["total"] / 2**40, 3)
            _bam = round(dir_dict[key]["bam"] / 2**40, 3)
            _cram = round(dir_dict[key]["cram"] / 2**40, 3)
            _vcf = round(dir_dict[key]["vcf"] / 2**40, 3)
            _pedbed = round(dir_dict[key]["pedbed"] / 2**40, 3)
            _unix_group = dir_dict[key]["group_name"]

            _pi = dir_dict[key]["pi"]

            _volume = dir_dict[key]["volume"]

            if _project[-1] != "/":
                # gets the total for the entire project
                _parenttotal = round(
                    dir_dict["/".join(key.split("/")[0:2])]["total"] / 2**40, 3)
            else:
                _parenttotal = _total

            instruction = ('''
            INSERT INTO spaceman_tmp (`Project`, `Directory`, `Volume`,
                `Files`, `Total`, `BAM`, `CRAM`, `VCF`, `PEDBED`,
                `Last Modified (days)`, `PI`, `Unix Group`, `Project Total`,
                `Status`, `Action`, `Comment`)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''')

            data = (_project, _path, _volume, _files, _total, _bam, _cram, _vcf,
                _pedbed, _mtime, _pi, _unix_group, _parenttotal, '', '', '')

            cursor.execute(instruction, data)

    # inserts new rows into the database, updates old rows if they exist
    cursor.execute('''
    REPLACE INTO spaceman
    SELECT db.`index`, report.Project, report.Directory, report.Volume,
        report.Files, report.Total, report.BAM, report.CRAM, report.VCF,
        report.PEDBED, report.`Last Modified (days)`, report.PI,
        report.`Project Total`, IFNULL(db.Status, "no decision"),
        IFNULL(db.`Action`, "no decision"), IFNULL(db.Comment, ""),
        report.`Unix Group`
    FROM spaceman_tmp AS report
    LEFT JOIN spaceman AS db
    USING(`Project`, `Directory`, `Volume`)
    ''')
    db_con.commit()

    # deletes rows which exist in the database but did not appear in the report
    cursor.execute('''
        DELETE FROM spaceman
        WHERE NOT EXISTS (
            SELECT null FROM spaceman_tmp AS report
            WHERE spaceman.Project = report.Project
            AND spaceman.Directory = report.Directory
            AND spaceman.Volume = report.Volume
        )
    ''')

    cursor.execute('''DELETE FROM spaceman_tmp''')
    db_con.commit()

    cursor.close()
    db_con.close()

    print("Updated spaceman's MySQL database.", file=sys.stderr)

def main():
    args = parser.parse_args()
    DEPTH = int(args.depth)+1
    if args.path is not None:
        FULL_PATH = pathlib.Path(args.path).resolve()
        FULL_PATH = str(FULL_PATH).strip("/")

        for key in PROJECT_DIRS:
            if re.match(key, FULL_PATH):
                _suffix = re.sub(key, '', FULL_PATH)
                FULL_PATH = PROJECT_DIRS[key] + _suffix

    HUMGEN_NAMES = getHumgenNames()

    dir_dict = {}
    if args.path is None:
        with multiprocessing.Pool() as pool:
            dictionaries = pool.starmap(createMapping,
                zip(ALL_PROJECTS.values(), repeat(HUMGEN_NAMES),
                repeat(DEPTH)))

        for dictionary in dictionaries:
            volume = list(dictionary.values())[0]["volume"]
            dir_dict[volume] = dictionary
    else:
        volume = FULL_PATH.split("/")[1][-3:]
        dir_dict[volume] = createMapping(FULL_PATH, HUMGEN_NAMES, DEPTH)

    if (args.tosql):
        updateSQL(dir_dict, volume)
    else:
        print("Note: all values are in TiB. Last Modified value is relative to mpistat time, and might be a few days behind.", file=sys.stderr)
        if (args.mode == "project" and args.header == True):
            print("Project\tDirectory\tTotal\tBAM\tCRAM\tVCF\tPED/BED\tFiles\tLast Modified (days)\tPI\tUnix Group\tVolume\tProject Total")
        elif (args.mode == "general" and args.header == True):
            print("Directory\tTotal\tBAM\tCRAM\tVCF\tPED/BED\tFiles\tLast Modified (days)\tTotal (bytes)")

        for volume in dir_dict:
            printTable(dir_dict[volume], volume, args.mode)

if __name__ == "__main__":
    main()
