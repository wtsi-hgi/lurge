#!/usr/bin/env bash

# We're going to submit a whole load of farm jobs to backfill a load of data

# Firstly, the MAX_DAYS_AGO variable must be set to a much larger value
# in directory_config.py

# Also, NOTE - the quota will not be backfilled, it'll just fill in as the 
# current quota.

# Also NOTE - you should ensure it can always access a base directory info
# file - maybe change `utils/finder.py` to use a current one if older ones
# don't exist

# Also, set the number of jobs in the array appropriately. Remeber, each run
# will go from that number of days back as far back as it can until it finds 
# some data to fill in. Only one should be run at a time, to avoid conflicts
# i.e. two simulataneous processes both adding the same group to the DB and
# oh look now it's there twice, but it should only be there once.

# Example: it's June 2022 and we want to fill in data back to November 2021.
# That's approximately 210 days. We know wrstat only outputs every four days
# if we're lucky, so at maximum, we need 52 runs. So we'll do that. It's not 
# precise maths, and we may get a few jobs at the end that quit instantly if
# they run over the max 210 days limit. But, it's good enough. :)

# Now the actual commands and not just comments explainig the process...

source /usr/local/lsf/conf/profile.lsf

export INSTANCE="dev"
SOFTWARE_ROOT="/software/hgi/installs/lurge"
export REPORT_DIR="/lustre/scratch119/humgen/teams/hgi/lurge/$INSTANCE"

bsub \
    -J "lurgeBackfill[1-52]%1" \
    -o $REPORT_DIR/backfill-logs/%J.%I.out \
    -e $REPORT_DIR/backfill-logs/%J.%I.out \
    -G hgi \
    -R "select[mem>3000] rusage[mem=3000]" -M 3000 -n 36 \
    "mpirun $SOFTWARE_ROOT/.venv/bin/python3 $SOFTWARE_ROOT/$INSTANCE/group_reporter.py --start-days-ago \$((\$LSB_JOBINDEX * 4))"