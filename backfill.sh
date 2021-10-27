#!/bin/bash
# This script is to call backfill.py to allow us tofill in loads of past data for a new scratch volume

# This will go back 100 days (change that in bsub -J)
# Make sure the MAX_DAYS_AGO setting in directory_config.py is set

# To go back to mpistat days
# - change the directory in the config
# - change the glob pattern in utils/finder
# multiply the days ago (input from job number) by four
# as wrstat only ran every four days at best, so it stops
# many concurrent jobs doing the same thing

source /usr/local/lsf/conf/profile.lsf
declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"

bsub -J "lurge[1-100]%1" -o "${REPORT_DIR}"new-lurge/report-logs/"$(date '+%Y-%m-%d')".%J.%I.out -e "${REPORT_DIR}"new-lurge/report-logs/"$(date '+%Y-%m-%d')".%J.err -G hgi -R "select[mem>3000] rusage[mem=3000] span[hosts=1]" -M 3000 -n 1 "${REPORT_DIR}.lurge_env/bin/python3 ${REPORT_DIR}new-lurge-dev/lurge/backfill.pyx \$LSB_JOBINDEX"