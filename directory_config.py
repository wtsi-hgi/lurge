# Global Config
WRSTAT_DIR = "/lustre/scratch114/teams/hgi/lustre_reports/wrstat/data/"
REPORT_DIR = "/lustre/scratch115/teams/hgi/lustre-usage/"
VOLUMES = [114, 115, 118, 119, 123]


# Manager Config
# max number of days ago to search for wrstat
MAX_DAYS_AGO = 10

# Reporter Config
# temporary sqlite database location
DATABASE_NAME = "/lustre/scratch115/teams/hgi/lustre-usage/_lurge_tmp_sqlite.db"

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

# Inspector Config
# redirects to actual storage locations
# also used in puppeteer config
PROJECT_DIRS = {
    'lustre/scratch115/projects': 'lustre/scratch115/realdata/mdt[0-9]/projects',
    'lustre/scratch119/humgen/projects': 'lustre/scratch119/realdata/mdt[0-9]/projects',
    'lustre/scratch115/teams': 'lustre/scratch115/realdata/mdt[0-9]/teams',
    'lustre/scratch119/humgen/teams': 'lustre/scratch119/realdata/mdt[0-9]/teams'
}

# where to search if no specific path given
ALL_PROJECTS = {
    '114': ["lustre/scratch114/projects", "lustre/scratch114/teams"],
    '115': ["lustre/scratch115/realdata/mdt[0-9]/projects", "lustre/scratch115/realdata/mdt[0-9]/teams"],
    '118': ["lustre/scratch118/humgen/hgi/projects", "lustre/scratch118/humgen/old-team-data"],
    '119': ["lustre/scratch119/realdata/mdt[0-9]/projects", "lustre/scratch119/realdata/mdt[0-9]/teams"],
    "123": ["lustre/scratch123/hgi/projects", "lustre/scratch123/hgi/teams"]
}
