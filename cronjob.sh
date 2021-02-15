#!/usr/bin/env bash
source /usr/local/lsf/conf/profile.lsf
set -eu

declare ROOT="/lustre/scratch119/humgen/teams/hgi/lurge"
declare LOG_DATE="$(date "+%Y-%m-%d")"

# Logging
exec 1>"${ROOT}/logs/cron.${LOG_DATE}.log"
exec 2>&1

declare -a TASKS=(
  "report"
  "inspect"
  "group-split"
)

for TASK in "${TASKS[@]}"; do
  declare LOG_FILE="${ROOT}/logs/${TASK}.${LOG_DATE}.%J.log"
  echo -n "Submitting ${TASK}: "

  # TODO Determine actual resource requirements
  bsub -G hgi -M 1000 -R "select[mem>1000] rusage[mem=1000]" \
       -o "${LOG_FILE}" -e "${LOG_FILE}" \
       "${ROOT}/${TASK}.sh"
done
