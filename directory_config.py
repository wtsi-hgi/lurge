import os

# Global Config
WRSTAT_DIR = "/lustre/scratch123/admin/team94/wrstat/output/"
REPORT_DIR = os.environ["REPORT_DIR"] + "/"
VOLUMES = [117, 118, 119, 123, 124, 125, 126]
LOGGING_CONFIG = "/software/hgi/installs/lurge/etc/logging.conf"

# Manager Config
# max number of days ago to search for wrstat
MAX_DAYS_AGO = 10

# Reporter Config
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
MDT_SYMLINKS = {
    "/lustre/scratch119/realdata/mdt[2-3]/teams": "/lustre/scratch119/humgen/teams",
    "/lustre/scratch119/realdata/mdt[2-3]/projects": "/lustre/scratch119/humgen/projects",
    "/lustre/scratch119/realdata/mdt[0-9]/casm": "/lustre/scratch119/casm",
    "/lustre/scratch123/hgi/mdt[0-9]/projects": "/lustre/scratch123/hgi/projects",
    "/lustre/scratch123/hgi/mdt[0-9]/teams": "/lustre/scratch123/hgi/teams"
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
