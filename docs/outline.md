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

- get humgen groups from ldap (`utils.ldap.py`)
- if a path isn't specified, spin up multiproc Pool workers to collect data on all the humgen directories we care about
    - firstly, finds the most recent mpistat report for the volume
    - iterates over every line in the mpistat
        - if the path doesn't contain a directory we care about, we skip it
        - if we're in a `users` directory, go one level deeper
        - if we have a directory:
            - find out the pi and group name
            - if the directory isn't already in our reports, add it and all its parents
            - add the PI and gorup to the directory report
        - else if we have a file:
            - get the PI and group information
            - if the directory (file) not in the reports (which it shouldn't be anyway), add it, and all its parent directories
            - backfill the file sizes, modified times etc.
            - if BAM/CRAM etc file, also backfill the file size in that category
        - return the collection of reports
    - if `tosql` flag set, we're going to write the information to the database (`db/common.py` and `db.inspector.py`)
        - First, we're going to load the PI/Group/Volume foreign keys into memory
        - Next, as we're replacing the old data, we're going to tag all the project_names with `.hgi.old.` at the start, instead of deleting it. This'll save us if the additions go wrong
        - For each DirectoryRecord we have, we're going to format the sizes nicely
        - We'll add anything to the foreign tables if neccesary
        - We're then going to add the information to the `directory` MySQL table, and get back the `directory_id`.
        - We can use that ID to then add all the BAM/CRAM etc data to the `file_size` table.
        - Finally, we can remove any old data - this is data tagged with `.hgi.old`
    - if we're not going to write it to the database, we're going to write it to `stdout` (`utils/table.py`)

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

