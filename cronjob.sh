#!/usr/bin/env bash
source /usr/local/lsf/conf/profile.lsf
set -euo pipefail

declare ROOT="/lustre/scratch119/humgen/teams/hgi/lurge"
declare MPISTAT_DIR="/lustre/scratch114/teams/hgi/lustre_reports/mpistat/data"

# Lustre scratch volumes we are interested in
declare -a VOLUMES=(114 115 116 117 118 119 123)

# Logging
exec 1>"${ROOT}/logs/cron.${LOG_DATE}.log"
exec 2>&1

latest_successul_run() {
  # Determine the latest successful run
  # Each successful run gets a sentinel file in the "successful"
  # directory, named after its date
  local latest="$(find "${ROOT}/successful" \
                       -type f -name "????????" -exec basename {} \; \
                  | sort -nr \
                  | head -1)"

  echo "${latest:-19700101}"
}

get_mpistat() {
  # Return the mpistat files that are newer than the given date
  # (tab-delimited date and scratch disk)
  local since="$(date -d "${1} + 1 day" "+%Y%m%d")"

  find "${MPISTAT_DIR}" -type f -name "????????_???.dat.gz" \
       \( -newermt "${since}" -not -newermt now \) \
       -exec basename {} .dat.gz \; \
  | tr '_' '\t'
}

latest_full_set() {
  # Get the date of the latest full set (per VOLUMES) of mpistat output
  # since the given date
  local since="$1"

  get_mpistat "${since}" \
  | sort -t$'\t' -k1nr,1 -k2n,2 \
  | awk '
    BEGIN {
      FS = OFS = "\t"
      volumes = ""
    }

    NR == 1 { now = $1 }

    $1 == now { volumes = volumes " " $2 }

    $1 != now {
      print now, volumes
      now = $1
      volumes = ""
    }

    END {
      print now, volumes
    }
  ' \
  | grep "${VOLUMES[*]}" \
  | head -1 \
  | cut -f1
}

main() {
  local since="$(latest_successul_run)"
  local latest

  if ! latest="$(latest_full_set "${since}")"; then
    >&2 echo "No full set of mpistat files exist since the last successful run (${since}) for:"
    >&2 printf "* scratch%s\n" "${VOLUMES[@]}"
    exit 1
  fi

  echo "Latest full set since ${since}: ${latest}"
}

main

#
#
#
#declare LOG_DATE="$(date "+%Y-%m-%d")"
#
#
#declare LATEST="$(find "${ROOT}/successful" \
#                       -type f -name "????????" -exec basename {} \; \
#                  | sort -nr \
#                  | head -1)"
#
#declare -a TASKS=(
#  "report"
#  "inspect"
#  "group-split"
#)
#
#for TASK in "${TASKS[@]}"; do
#  declare LOG_FILE="${ROOT}/logs/${TASK}.${LOG_DATE}.%J.log"
#  echo -n "Submitting ${TASK}: "
#
#  # TODO Determine actual resource requirements
#  bsub -G hgi -M 1000 -R "select[mem>1000] rusage[mem=1000]" \
#       -o "${LOG_FILE}" -e "${LOG_FILE}" \
#       "${ROOT}/${TASK}.sh"
#done
