#!/usr/bin/env bash

# where the mpistat output files are found
declare MPISTAT_DIR="/lustre/scratch114/teams/hgi/lustre_reports/mpistat/data/"
#declare MPISTAT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/wrstat2mpistat/"
# where the reports are generated, should usually also be current working dir
declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"
declare PY_ENV="/lustre/scratch115/teams/hgi/lustre-usage/.lurge_env/bin/"

# how many days back the script will look for sets
declare max_days_ago=10

declare days_ago=0
declare success_flag=0
declare MPI_DATE=""
# scratch volumes to actually scan
declare -a volumes=(114 115 118 119)

all_exist() {
  local date="$1"
  shift

  while (( $# )); do
    [[ -e "${MPISTAT_DIR}${date}_${1}.dat.gz" ]] || return 1
    shift
  done

  return 0
}

while [[ ${days_ago} -lt ${max_days_ago} ]] && [[ ${success_flag} -eq 0 ]];
# while (days_ago < max_days_ago) or (sucess_flag != 1)
do
	# Finds the date, formatted as YYYYMMDD, ${days_ago} days ago from today
	MPI_DATE=$(date -d ${days_ago}' days ago' '+%Y%m%d')
	echo "Looking for reports at ${MPI_DATE}..."
	# Finds most recent date for which there is a full set of mpistat outputs.
	# Not strictly relevant to the program, but it's a quick way of telling
	# when the last full mpistat output was.

	if [[ $(find "${REPORT_DIR}report-output-files" -name "report-${MPI_DATE}.tsv" -exec echo 1 \; | wc -l) -ne 0 ]];
	then
		success_flag=2;
		echo "Can't find mpistat output more recent than report-${MPI_DATE}.tsv!";
	else
		if ! all_exist "${MPI_DATE}" "${VOLUMES[@]}"
		then
			((++days_ago));
		else
			"${PY_ENV}"python3 "${REPORT_DIR}"project_inspector.py --tosql
			success_flag=1
		fi;
	fi;
done

if [ ${days_ago} -ge ${max_days_ago} ]; # if (days_ago >= max_days_ago)
then
	echo "No usable mpistat output sets from the last ${days_ago} days found!";
fi;
