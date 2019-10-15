#!/usr/bin/env bash

# where the mpistat output files are found
declare MPISTAT_DIR="/lustre/scratch114/teams/hgi/lustre_reports/mpistat/data/"
# where the reports are generated, should usually also be current working dir
declare REPORT_DIR="/lustre/scratch115/teams/hgi/lustre-usage/"
declare PY_ENV="/lustre/scratch115/teams/hgi/lustre-usage/.lurge_env/bin/"
# how many scratchXYZ volumes there are (that mpistat creates output for)
declare SCRATCH_COUNT=6
# how many days back the script will look for sets
declare max_days_ago=7

declare days_ago=0
declare success_flag=0
declare MPI_DATE=""
# scratch volumes to actually scan
declare volumes=(114 115 118 119)

while [[ ${days_ago} -lt ${max_days_ago} ]] && [[ ${success_flag} -eq 0 ]];
# while (days_ago < max_days_ago) or (sucess_flag != 1)
do
	# Finds the date, formatted as YYYYMMDD, ${days_ago} days ago from today
	MPI_DATE=$(date -d ${days_ago}' days ago' '+%Y%m%d')
	echo "Looking for reports at ${MPI_DATE}..."
	# Finds most recent date for which there is a set of mpistat outputs matching
	# the 'volumes' list.
	# Looks at number of files found, searching based on the filename date
	# starting at [today's date]_(num).dat.gz and heading back a day for each loop

	# Makes sure not to run the report scripts if the most recent mpistat output
	# files are as old as the most recent report
	if [[ $(find "${REPORT_DIR}" -name "report-${MPI_DATE}.tsv" -exec echo 1 \; | wc -l) -ne 0 ]];
	then
		success_flag=2;
		echo "Can't find mpistat output more recent than report-${MPI_DATE}.tsv!";
	else
		if [[ $(find "${MPISTAT_DIR}" -name "${MPI_DATE}_*.dat.gz" -exec echo 1 \; | wc -l) != "${SCRATCH_COUNT}" ]];
		then
			((++days_ago));

		else
			# when a full set of mpistat files is found, create a set of links called
			# "latest-{volume}.dat.gz" in the report directory
			declare -a filenames

			for volume in ${volumes[@]};
			do
				ln -fs "${MPISTAT_DIR}${MPI_DATE}_${volume}.dat.gz" "${REPORT_DIR}latest-${volume}.dat.gz"
				# iteratively extend an array of file names
				filenames=("${filenames[@]}" "latest-${volume}.dat.gz")
			done
			echo "Created links to latest reports..."

			echo "Starting report generator..."
			declare DATE_FORMATTED=$(date -d ${days_ago}' days ago' '+%Y-%m-%d')
			# invokes the report generator, passing the filenames as arguments
			"${PY_ENV}"python3 "${REPORT_DIR}"report.py "${DATE_FORMATTED}" "${filenames[@]}"
			success_flag=1
		fi;
	fi;
done

if [ ${days_ago} -ge ${max_days_ago} ]; # if (days_ago >= max_days_ago)
then
	echo "No usable mpistat output sets from the last ${days_ago} days found!";
fi;
