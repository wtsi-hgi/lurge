# Outline

## Scripts

#### `cron.sh`

- bsub: `manager.py`: `both` flag - (`both` flag will run the `report.py` and then `inspector.py`)

#### `manager.py`

- tries to determine latest full set of mpistat
- only looks over the past `MAX_DAYS_AGO`
- won't proceed if report-DATE.tsv exists
- if running the reporter
    - symlinks mpistat locally for convenienve
- runs `reporter.py` or `inspector.py` or both as required

#### `report.py`

- creates temporary sqlite database on disk
- estableses connection to MySQL (`db/common.py`)
- checks mysql database for date passed to `report` (`db/report.py`) - aborts if found
- estableshes connection to Sanger LDAP (`utils/ldap.py`)
- creates sqlite table of humgen ldap groups, with gid, name and PI (`utils/ldap.py`)
- creates sqlite tables for each possible volume
- runs the following for each mpistat input (multiproc pool)
    - creates a new connection to the sqlite db (stops any issues re concurrent editing)
    - reads mpistat filename to determine volume
    - deserialises group table into memory
    - iterates over the mpistat file, accumalating group data into memory
    - for each group accumalated:
      - fill in blanks from ldap if neccesary
      - attempt to get quota and consumption by running `lfs quota`
      - check for archived directories (old .imirrored file)
      - returns the group data for the volume
    - adds all the group data for all the volumes to the sqlite database
    - transfers the data to the mysql database (`db/report.py`)
    - writes output to `.tsv` file (`utils/tsv.py`)
    - removes sqlite file

#### `project_inspector.py`

- TODO
```
* `inspector_cron.sh` (old, reference for new `project_inspector.py`)
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
```
### `group_splitter_cron.sh`
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

