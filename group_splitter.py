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
WORKING_DIR = ""

parser = argparse.ArgumentParser(description="Splits mpistat output by Unix group into files in the current directory. Files are named by group names by default, use the --id flag to use group IDs instead.")

parser.add_argument('--id', '-i', dest='name', action='store_const',
    const=False, default=True, help="Use group IDs for files instead of" \
        "group names.")

parser.add_argument('--output', '-o', dest='output', type=str, nargs='?',
    default="groups/",
    help="Output directory for split files.")

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
        print("Warning, couldn't find mpistat output for today. Using mpistat output for {0:%Y-%m-%d} instead.".format(report_date), file=sys.stderr)

    return REPORT_DIR+filename

def generateIndex(stats):
    global WORKING_DIR
    with open(WORKING_DIR + "index.txt", 'wt') as index:
        index.write("Group\tBuild time (sec)\tMemory use (bytes)\n")
        for group in stats.keys():
            # The magic numbers used here have been found by running Treeserve
            # on a bunch of different files and looking at the patterns.
            # They're not an exact science but they give a good enough
            # pessimistic estimate to be useful.

            # Expected lines/second throughput of Treeserve
            lines_per_second = 11000
            # Expected time taken to launch an Openstack instance and prepare
            # Treeserve to run
            instantiation_overhead = 100 #seconds
            # Expected number of additional nodes on top of dir count * 2
            extra_nodes = 50

            build_time = stats[group]['lines'] / lines_per_second
            build_time += instantiation_overhead

            # The memory per node is heavily dependent on how many of the total
            # lines are directories.
            dir_percentage = (stats[group]['dirs'] / stats[group]['lines'])*100

            if dir_percentage < 0.8:
                bytes_per_node = 12000
            elif dir_percentage < 1.5:
                bytes_per_node = 10000
            elif dir_percentage < 2:
                bytes_per_node = 9000
            elif dir_percentage < 4:
                bytes_per_node = 8500
            elif dir_percentage >= 4:
                bytes_per_node = 8000

            # The number of nodes is always just a bit more than 2*dir count
            node_count = stats[group]['dirs'] * 2 + extra_nodes

            memory_use = node_count * bytes_per_node

            index.write("{}\t{}\t{}\n".format(group, build_time, memory_use))

def main():
    args = parser.parse_args()
    global WORKING_DIR
    # resolve the output directory and convert it back to str for use by open()
    WORKING_DIR = str(pathlib.Path(args.output).resolve()) + "/"
    print("Writing split files to {}".format(WORKING_DIR), file=sys.stderr)

    reports = []

    for scratch in SCRATCHES:
        reports.append(findReport(scratch))

    HUMGEN_GROUPS = getHumgenIDs()
    opened_files = {}
    # used to create an index of groups showing expected time and ram
    # requirements for treeserve
    stats = {}

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
                group_name = HUMGEN_GROUPS[gid]

                if group_name == "":
                    continue

                if args.name == True:
                    group_file = group_name + ".dat.gz"
                else:
                    group_file = gid + ".dat.gz"

                if group_file not in opened_files.keys():
                    opened_files[group_file] = gzip.open(
                        WORKING_DIR + group_file, 'wt')
                    stats[group_name] = {'lines': 0, 'dirs': 0}

                opened_files[group_file].write(line)

                stats[group_name]['lines'] += 1
                if split_line[7] == "d":
                    stats[group_name]['dirs'] += 1

                lines_written += 1

    for file in opened_files.keys():
        opened_files[file].close()

    generateIndex(stats)
    print("Splitting finished.", file=sys.stderr)

if __name__ == "__main__":
    main()
