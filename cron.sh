#!/bin/bash
source /usr/local/lsf/conf/profile.lsf
declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"

bsub -o "${REPORT_DIR}"new-lurge/report-logs/"$(date '+%Y-%m-%d')".%J.out -e "${REPORT_DIR}"new-lurge/report-logs/"$(date '+%Y-%m-%d')".%J.err -G hgi -R "select[mem>3000] rusage[mem=3000] span[hosts=1]" -M 3000 -n 5 "${REPORT_DIR}.lurge_env/bin/python3 ${REPORT_DIR}new-lurge/lurge/manager.py reporter inspector puppeteer users splitter"