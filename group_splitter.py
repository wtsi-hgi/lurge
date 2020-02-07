#!/usr/bin/env python3
import gzip
import os
import re
import base64
import argparse
import sys
import pathlib
import datetime

import ldap

PROJECT_DIRS = {
    'lustre/scratch115/projects': '/lustre/scratch115/realdata/mdt[0-9]/projects',
    'lustre/scratch119/humgen/projects': '/lustre/scratch119/realdata/mdt[0-9]/projects'
}
REPORT_DIR = "/lustre/scratch114/teams/hgi/lustre_reports/mpistat/data/"
SCRATCHES = ["/lustre/scratch114", "/lustre/scratch115", "/lustre/scratch118", "/lustre/scratch119"]

parser = argparse.ArgumentParser(description="Splits mpistat output by Unix group into files in the current directory. Files are named by group names by default, use the --id flag to use group IDs instead.")

parser.add_argument('--id', '-i', dest='name', action='store_const',
    const=False, default=True, help="Use group IDs for files instead of" \
        "group names.")

def getHumgenIDs():
    con = ldap.initialize("ldap://ldap-ro.internal.sanger.ac.uk:389")
    con.bind('','')

    results = con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
        ldap.SCOPE_ONELEVEL, "(objectClass=sangerHumgenProjectGroup)",
        ['gidNumber', 'cn'])

    groups = {}

    for entry in results:
        gid = entry[1]['gidNumber'][0].decode("UTF-8", "replace")
        gname = entry[1]['cn'][0].decode("UTF-8", "replace")
        groups[gid] = gname

    return groups

def find_report(dir):
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
        print("Warning, couldn't find mpistat output for today. Using mpistat output for {0:%Y-%m-%d} instead.".format(report_date), file=sys.stderr)

    return REPORT_DIR+filename

def main():
    args = parser.parse_args()
    reports = []

    for scratch in SCRATCHES:
        reports.append(find_report(scratch))

    HUMGEN_GROUPS = getHumgenIDs()
    opened_files = {}

    lines_read = 0
    lines_written = 0

    for report_path in reports:
        print("Reading {}...".format(report_path), file=sys.stderr)
        with gzip.open(report_path, 'rt') as mpistat:
            for line in mpistat:
                lines_read += 1

                if lines_read % 500000 == 0:
                    print("{} lines read, {} lines written".format(lines_read, lines_written))

                split_line = line.split()

                gid = split_line[3]

                if gid not in list(HUMGEN_GROUPS.keys()):
                    continue

                if args.name == True:
                    group_file = "split/" + HUMGEN_GROUPS[gid] + ".dat.gz"
                else:
                    group_file = "split/" + gid + ".dat.gz"

                if group_file not in opened_files.keys():
                    opened_files[group_file] = gzip.open(group_file, 'aw')

                opened_files[group_file].write(line)
                lines_written += 1

    for file in opened_files.keys():
        opened_files[file].close()


if __name__ == "__main__":
    main()
