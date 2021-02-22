# Outline

Trying to understand how the pipeline works

## Scripts

* `report_cron.sh`
  * bsub: `reportmanager.sh`
    * tries to determine latest full set of mpistat
    * only looks over the past N days
    * won't proceed if report-DATE.tsv exists
    * symlinks mpistat locally for convenience
    * pass to: `report.py` with DATE and SYMLINKS
      * creates temporary sqlite database on disk
      * checks mysql database for DATE -- aborts if found
      * creates sqlite table of humgen ldap groups, with gid, name and PI surname
      * runs the following for each mpistat input (multiproc pool)
        * reads first line to determine volume
        * deserialises group table into memory
        * iterate through mpistat file, accumulating into memory
          * size, per group
          * last modified, per group
        * create table named after volume
        * for each group accumulated
          * fill in group blanks from ldap, if necessary
          * attempt to get quota and consumption by parsing `lfs quota`
          * check for archived directories (old .imirrored file)
          * dump into table
      * dump sqlite tables into mysql table, with run date
      * dump sqlite tables into report-DATE.tsv

* `inspector_cron.sh`
  * bsub: `inspector_manager.sh`
    * tries to determine latest full set of mpistat
    * only looks over the past N days
    * won't proceed if report-DATE.tsv exists
    * pass to: `project_inspector.py` with `--tosql`
      * `--tosql` will write to the MySQL DB, as well as stdout
      * If given a path (mutually exclusive with `--tosql`), then apply
        mapping for Lustres with multiple MDTs
      * get humgen groups from ldap (gid, name and PI), deref PI dn to
        fetch surname
      * Create multiprocess pool and to create mapping for path/all
        project roots
        * Finds the latest mpistat output for the scratch volume by
          starting at today and working backwards until something's
          found
        * Iterate through mpistat file
          * If the path matches what we're looking for:
            * If we have a directory
              * Normalise `users` directories, if they exist and it's
                possible, to `users/whatever`
              * Record in dictionary, if it doesn't exist; also add its
                parents to dictionary, if they don't exist. Set all
                accumulators for entry to 0, except files (to 1)
              * Set PI and group name for entry
            * If we have a file
              * Go a level deeper if we're in a `users` directory, when
                possible
              * Record in dictionary, if it doesn't exist, with
                appropriate mtime, pi and group; do the same for parents
                (if they don't exist)
              * Aggregate size and files; update mtime if newer. Do this
                for all parents in dictionary
              * If the file matches one of the interesting classes (bam,
                cram, etc.) then aggregate for those and its parents
        * Write to DB, if required, otherwise print to stdout

* `group_splitter_cron.sh`
  * bsub: `group_splitter_manager.sh`
    * `group_splitter.py`
      * Finds latest mpistat file for each volume its interested in
      * Gets all groups and all humgen groups from ldap
      * Iterate through each mpistat file:
        * Decode file path
        * If gid isn't humgen or special (116/vr or tol) then skip
        * Gets group name by gid; if it can't, then skip
        * Open the output file for the group, if it hasn't been already
        * Write the input to the output file
        * Take stats (number of records and number of directories)
      * Create an "index file" from the group/records/directories stats
        based on anecdotal timing (this is to estimate how long
        TreeServe takes to start)
    * Dumps passwd and group databases to files
    * Uploads split mpistat files, passwd and group databases to S3

## MySQL Database

### `lustre_usage`

Used by Weaver

| Field                | Type    | Constraints |
| :------------------- | :------ | :---------- |
| id                   | int     | PK auto     |
| Lustre Volume        | varchar | Unique A    |
| PI                   | varchar |             |
| Unix Group           | varchar | Unique A    |
| Used (bytes)         | bigint  |             |
| Quota (bytes)        | bigint  |             |
| Consumption          | varchar |             |
| Last Modified (days) | decimal |             |
| Archived Directories | varchar |             |
| date                 | date    | Unique A    |
| isHumgen             | tinyint |             |

### `spaceman` and `spaceman_tmp`

Used by Spaceman

| Field                | Type    | Constraints |
| :------------------- | :------ | :---------- |
| index                | int     | PK auto     |
| Project              | varchar |             |
| Directory            | varchar |             |
| Volume               | varchar |             |
| Files                | bigint  |             |
| Total                | float   |             |
| BAM                  | float   |             |
| CRAM                 | float   |             |
| VCF                  | float   |             |
| PEDBED               | float   |             |
| Last Modified (days) | float   |             |
| PI                   | varchar |             |
| Project Total        | float   |             |
| Status               | varchar |             |
| Action               | varchar |             |
| Comment              | varchar |             |
| Unix Group           | varchar |             |
