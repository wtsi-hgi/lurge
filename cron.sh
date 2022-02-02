#!/bin/bash
source /usr/local/lsf/conf/profile.lsf
declare REPORT_DIR="/lustre/scratch119/humgen/teams/hgi/lurge/lurge/"

bsub -o "${REPORT_DIR}"report-logs/"$(date '+%Y-%m-%d')".%J.out -e "${REPORT_DIR}"report-logs/"$(date '+%Y-%m-%d')".%J.err -G hgi -R "select[mem>3000] rusage[mem=3000] span[hosts=1]" -M 3000 -n 5 "${REPORT_DIR}lurge/.venv/bin/python3 ${REPORT_DIR}lurge/manager.py reporter inspector puppeteer users splitter"
