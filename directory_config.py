import os

# Global Config
WRSTAT_DIR = "/lustre/scratch123/admin/team94/wrstat/output/"
REPORT_DIR = os.environ["REPORT_DIR"] + "/"
VOLUMES = [119]
#VOLUMES = [117, 118, 119, 123, 124]
LOGGING_CONFIG="/software/hgi/installs/lurge/etc/logging.conf"

# Manager Config
# max number of days ago to search for wrstat
MAX_DAYS_AGO = 20

# Reporter Config
# directories with group directories to scan for .imirrored
GROUP_DIRECTORIES = {
    "scratch117": ["/lustre/scratch117/casm", "/lustre/scratch117/casm"],
    'scratch118': ["/lustre/scratch118/humgen/old-team-data/",
                   "/lustre/scratch118/humgen/hgi/projects/"],
    'scratch119': ["/lustre/scratch119/humgen/teams",
                   "/lustre/scratch119/humgen/projects/", "/lustre/scratch119/casm/"],
    "scratch123": ["/lustre/scratch123/hgi/teams/", "/lustre/scratch123/hgi/projects/"],
    "scratch124": ["/lustre/scratch124/casm", "/lustre/scratch124/casm"]
}

# Warning System
# Default warning is the 'OK' level
# WARNINGS contains dictionary of warning levels to a
# set of thresholds: (days from now, amount of quota to exceed)
# The levels here must match the DB
DEFAULT_WARNING = 1
WARNINGS = {
    2: {(7, 0.85)},
    3: {(3, 0.8), (7, 0.95)}
}

# Inspector Config
# redirects to actual storage locations
# also used in puppeteer config
PROJECT_DIRS = {
    'lustre/scratch119/humgen/projects': 'lustre/scratch119/realdata/mdt[0-9]/projects',
    'lustre/scratch119/humgen/teams': 'lustre/scratch119/realdata/mdt[0-9]/teams',
    "lustre/scratch119/casm": "lustre/scratch119/realdata/mdt[0-9]/casm",
    "lustre/scratch123/hgi/projects": "lustre/scratch123/hgi/mdt[0-9]/projects",
    "lustre/scratch123/hgi/teams": "lustre/scratch123/hgi/mdt[0-9]/teams"
}

# where to search if no specific path given
ALL_PROJECTS = {
    "117": ["lustre/scratch117/casm/team113"],
    '118': ["lustre/scratch118/humgen/hgi/projects", "lustre/scratch118/humgen/old-team-data"],
    '119': ["lustre/scratch119/realdata/mdt[0-9]/projects", 
            "lustre/scratch119/realdata/mdt[0-9]/teams", 
            "lustre/scratch119/realdata/mdt[0-9]/casm"
            ],
    "123": ["lustre/scratch123/hgi/mdt[0-9]/projects", "lustre/scratch123/hgi/mdt[0-9]/teams"],
    "124": ["lustre/scratch124/casm/team113"]
}

# Filetypes
# This is {what it should display as: the regex to match}
FILETYPES = {
    "SAM": "\.(sam)(\.gz)?$",
    "BAM": "\.(bam)(\.gz)?$",
    "FASTQ": "\.(fastq)$",
    "FASTQ (gzip)": "\.(fastq)(\.gz)$",
    "CRAM": "\.(cram)(\.gz)$",
    "VCF": "\.(vcf|bcf|gvcf)$",
    "VCF (gzip)": "\.(vcf|bcf|gvcf)(\.gz)$",
    "PEDBED": "\.(ped|bed)(\.gz)?$"
}


class Treeserve:
    # Group Splitter Treeserve Info

    # this still references mpistat, do we want to change that?
    S3_UPLOAD_LOCATION = "s3://branchserve/mpistat/"

    LINES_PER_SECOND = 11000
    OVERHEAD_SECS = 100
    EXTRA_NODES = 50

    # Format is list of pairs (max percent: bytes per node)
    # This list MUST be in ascending percentage order
    # 100 is used as the default value
    BYTES_PER_NODE_BY_DIR_PERCENT = [
        (0.8, 12000),
        (1.5, 10000),
        (2, 9000),
        (4, 8500),
        (100, 8000)
    ]


MAX_LINES_PER_GROUP_PER_VOLUME = 50

# pseudo groups for extra non-humgen groups we want to find out about
# {start of file path: (pseudo-group number (negative), group name, pi name)}
# Group Names CANNOT be the same as an already existing group
# becuase it creates conflicts
PSEUDO_GROUPS = {
    "/lustre/scratch117/casm/team113": (-1, "team113-116", "Team 113"),
    "/lustre/scratch119/realdata/mdt1/team113": (-1, "team113-116", "Team 113"),
    "/lustre/scratch124/casm/team113": (-1, "team113-116", "Team 113")
}
