# Global Config
WRSTAT_DIR = "/lustre/scratch114/teams/hgi/lustre_reports/wrstat/data/"
REPORT_DIR = "/lustre/scratch115/teams/hgi/lustre-usage/new-lurge/"
VOLUMES = [114, 115, 118, 119, 123]
LOGGING_CONFIG = "/lustre/scratch115/teams/hgi/lustre-usage/new-lurge/lurge/logging.conf"


# Manager Config
# max number of days ago to search for wrstat
MAX_DAYS_AGO = 10

# Reporter Config
# directories with group directories to scan for .imirrored
GROUP_DIRECTORIES = {
    'scratch114': ["/lustre/scratch114/teams/", "/lustre/scratch114/projects/"],
    'scratch115': ["/lustre/scratch115/teams/", "/lustre/scratch115/projects/"],
    'scratch118': ["/lustre/scratch118/humgen/old-team-data/",
                   "/lustre/scratch118/humgen/hgi/projects/"],
    'scratch119': ["/lustre/scratch119/humgen/teams",
                   "/lustre/scratch119/humgen/projects/"],
    "scratch123": ["/lustre/scratch123/hgi/teams/", "/lustre/scratch123/hgi/projects/"]
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
    'lustre/scratch115/projects': 'lustre/scratch115/realdata/mdt[0-9]/projects',
    'lustre/scratch119/humgen/projects': 'lustre/scratch119/realdata/mdt[0-9]/projects',
    'lustre/scratch115/teams': 'lustre/scratch115/realdata/mdt[0-9]/teams',
    'lustre/scratch119/humgen/teams': 'lustre/scratch119/realdata/mdt[0-9]/teams',
    "lustre/scratch123/hgi/projects": "lustre/scratch123/hgi/mdt[0-9]/projects",
    "lustre/scratch123/hgi/teams": "lustre/scratch123/hgi/mdt[0-9]/teams"
}

# where to search if no specific path given
ALL_PROJECTS = {
    '114': ["lustre/scratch114/projects", "lustre/scratch114/teams"],
    '115': ["lustre/scratch115/realdata/mdt[0-9]/projects", "lustre/scratch115/realdata/mdt[0-9]/teams"],
    '118': ["lustre/scratch118/humgen/hgi/projects", "lustre/scratch118/humgen/old-team-data"],
    '119': ["lustre/scratch119/realdata/mdt[0-9]/projects", "lustre/scratch119/realdata/mdt[0-9]/teams"],
    "123": ["lustre/scratch123/hgi/mdt[0-9]/projects", "lustre/scratch123/hgi/mdt[0-9]/teams"]
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
