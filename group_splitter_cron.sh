#!/bin/bash
source /usr/local/lsf/conf/profile.lsf
declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"

bsub -o "${REPORT_DIR}/group-splitter-logs/$(date '+%Y-%m-%d').%J.out" -G hgi -R "select[mem>1000] rusage[mem=1000] span[hosts=1]" -M 1000 -n 1 "${REPORT_DIR}group_splitter_manager.sh"
