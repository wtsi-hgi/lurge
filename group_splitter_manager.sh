#!/usr/bin/env bash

declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"
declare PY_ENV="/lustre/scratch115/teams/hgi/lustre-usage/.lurge_env/bin/"

"${PY_ENV}"python3 "${REPORT_DIR}"group_splitter.py -o "${REPORT_DIR}/groups/"
s3cmd sync "${REPORT_DIR}groups/" "s3://branchserve/mpistat/"
