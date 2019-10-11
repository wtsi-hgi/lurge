#!/bin/bash
source /usr/local/lsf/conf/profile.lsf
declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"

bsub -o "${REPORT_DIR}"report-logs/"$(date '+%Y-%m-%d')".%J.out -e "${REPORT_DIR}"report-logs/"$(date '+%Y-%m-%d')".%J.err -G hgi -R "select[mem>200] rusage[mem=200] span[hosts=1]" -M 200 -n 4 "${REPORT_DIR}"reportmanager.sh
