#!/bin/bash
source /usr/local/lsf/conf/profile.lsf
declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"

bsub -o "${REPORT_DIR}"inspector-logs/"$(date '+%Y-%m-%d')".%J.out -e "${REPORT_DIR}"inspector-logs/"$(date '+%Y-%m-%d')".%J.err -G hgi -R "select[mem>500] rusage[mem=500] span[hosts=1]" -M 500 -n 4 "${REPORT_DIR}"inspector_manager.sh
