#!/usr/bin/env bash

declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"
declare PY_ENV="/lustre/scratch115/teams/hgi/lustre-usage/.lurge_env/bin/"

"${PY_ENV}"python3 "${REPORT_DIR}"group_splitter.py -o "${REPORT_DIR}/groups/"

declare tries=0
while true; do
    s3cmd sync "${REPORT_DIR}groups/" "s3://branchserve/mpistat/"
    if [ $? -eq 0 ]; then
        break
    fi
    if [ ${tries} -ge 5 ]; then
        echo "s3cmd sync failed five times in a row. Aborting..."
        break
    fi
    echo "s3cmd sync failed, retrying in two seconds..."
    tries+=1
    sleep 2
done
