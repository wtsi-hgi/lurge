#!/usr/bin/env bash
source /usr/local/lsf/conf/profile.lsf

INSTANCE="prod"
# INSTANCE="dev"

SOFTWARE_ROOT="/software/hgi/installs/lurge"
export REPORT_DIR="/lustre/scratch119/humgen/teams/hgi/lurge/$INSTANCE"

run_splitter=""
[[ $INSTANCE == "prod" ]] && run_splitter="splitter"

bsub -o $REPORT_DIR/report-logs/$(date '+%Y-%m-%d').%J.out -e $REPORT_DIR/report-logs/$(date '+%Y-%m-%d').%J.err -G hgi -R "select[mem>3000] rusage[mem=3000] span[hosts=1]" -M 3000 -n 5 "$SOFTWARE_ROOT/.venv/bin/python3 $SOFTWARE_ROOT/$INSTANCE/manager.py reporter inspector puppeteer users $run_splitter"
