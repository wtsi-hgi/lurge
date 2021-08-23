#!/bin/bash
source /usr/local/lsf/conf/profile.lsf
declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"

bsub -o "${REPORT_DIR}"report-logs/"$(date '+%Y-%m-%d')".%J.out -e "${REPORT_DIR}"report-logs/"$(date '+%Y-%m-%d')".%J.err -G hgi -R "select[mem>500] rusage[mem=500] span[hosts=1]" -M 500 -n 5 "${REPORT_DIR}.lurge_env/bin/python3 ${REPORT_DIR}manager.py both"