# Outline

## Scripts

### `cron.sh`

- bsub: `manager.py`: all parameters passed are the lurge modules to run (`reporter`, `inspector` and `puppeteer`)

### `manager.py`

- runs `reporter.py`, `inspector.py` or `puppeteer.py` as required

### `report.py`

- creates temporary sqlite database on disk
- estableses connection to MySQL (`db/common.py`)
- finds the latest mpistat for each volume, and checks if its already in the database. we only care if its new data (`utils/finder.py` and `db/report.py`)
- estableshes connection to Sanger LDAP (`utils/ldap.py`)
- creates sqlite table of humgen ldap groups, with gid, name and PI (`utils/ldap.py`)
- creates sqlite tables for each possible volume
- runs the following for each mpistat input (multiproc pool)
    - creates a new connection to the sqlite db (stops any issues re. concurrent editing)
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

### `project_inspector.py`

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

### `puppeteer.py`

- if not passed volumes to use, uses all volumes
- starts multiprocessing processes for how many mpistats it has to read over
    - finds the most recent mpistat for each volume (`utils/finder.py`)
    - checks that it hasn't already got that data in the DB, otherwise it'll skip that particular mpistat
    - iterates over mpistat file for first time (finding vaults)
        - if it finds the deepest level in a vault (contains `.vault` and `XXX-YYY`), it gets the relative path of the file from the `.vault`
        - using the relative path, and the path of the `.vault`, it can produce the full path
        - it can also get the inode out from the vault path
        - it creats a `VaultPuppet` with the information it has at the moment (`lurge_types/vault.py`)
    - iterates over mpistat file for the second time (finding files affected by vaults)
        - if the inode of the file was used in a vault, fill out the `VaultPuppet` with more information, this time from the actual file itself
    - creates a LDAP connection, and asks it for the HumGen groups (`utils/ldap.py`)
    - lets the VaultPuppet tidy itself up, and fill out extra details, such as using LDAP (`lurge_types/vault.py`) and replacing the full filepath with a human readable one
- creates a SQL connection to the database
- writes all its info to the database (`db/puppeteer.py`)
    - gets groups, volumes and actions (Keep, Archive) from the database for their foreign keys
    - iterates over all volumes and VaultPuppets
        - if its not an action we care about, skip it
        - if the group or volume doesn't exist in the database - create it
        - adds the VaultPuppet as a record in the `vault` table

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

