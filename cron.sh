#!/usr/bin/env bash
source /usr/local/lsf/conf/profile.lsf

# INSTANCE="prod"
INSTANCE="dev"

export INSTANCE

SOFTWARE_ROOT="/software/hgi/installs/lurge"
export REPORT_DIR="/lustre/scratch119/humgen/teams/hgi/lurge/$INSTANCE"

run_splitter=""
[[ $INSTANCE == "prod" ]] && run_splitter="splitter"

bsub \
    -o $REPORT_DIR/report-logs/$(date '+%Y-%m-%d').%J.out \
    -e $REPORT_DIR/report-logs/$(date '+%Y-%m-%d').%J.err \
    -G hgi \
    -R "select[mem>3000] rusage[mem=3000] span[hosts=1]" -M 3000 -n 5 \
    "$SOFTWARE_ROOT/.venv/bin/python3 $SOFTWARE_ROOT/$INSTANCE/manager.py puppeteer users $run_splitter"

# Run main reporter with MPI
# NUM_CPUs is 1 + number of volumes * (WORKERS_PER_VOLUME + 1)
# WORKERS_PER_VOLUME is defined towards top of group_reporter.py
# i.e. 5 volumes (117, 118, 119, 123, 124), 6 workers per volume = 36 cores 
NUM_CPUs=36

export LD_LIBRARY_PATH=/software/openmpi-4.0.3/lib:$LD_LIBRARY_PATH
export PATH=/software/openmpi-4.0.3/bin:$PATH

bsub \
    -o $REPORT_DIR/report-logs/$(date '+%Y-%m-%d').%J.group_report.out \
    -e $REPORT_DIR/report-logs/$(date '+%Y-%m-%d').%J.group_report.err \
    -G hgi \
    -R "select[mem>3000] rusage[mem=3000]" -M 3000 -n $NUM_CPUs \
    "mpirun $SOFTWARE_ROOT/.venv/bin/python3 $SOFTWARE_ROOT/$INSTANCE/group_reporter.py"
    

