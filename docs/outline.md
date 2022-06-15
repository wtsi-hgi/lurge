# Outline

## Scripts

### `cron.sh`

- bsub: `manager.py`: all parameters passed are the lurge modules to run (`reporter`, `puppeteer` and `users`)
- also runs the project_inspector separetly through an `mpirun` call

### `manager.py`

- runs `reporter.py`, `inspector.py`, `puppeteer.py` or `user_reporter.py` as required

### `report.py`


- establishes connection to MySQL (`db/common.py`)
- finds the latest wrstat for each volume, and checks if its already in the database. we only care if its new data (`utils/finder.py` and `db/report.py`)
- estableshes connection to Sanger LDAP (`utils/ldap.py`)
- collects ldap groups, with gid, name and PI (`utils/ldap.py`)
- runs the following for each wrstat input (multiproc pool)
    - reads wrstat filename to determine volume
    - deserialises group table into memory
    - iterates over the wrstat file, accumalating group data into memory. this is stored in GroupReport objects (`lurge_types/group_report.py`)
        - it checks each record to see if it starts with a filepath defined in the PSEUDO_GROUPS config. If so, it overwrites the group on the record with the pseudo group. This allows us to get data sorted by filepath instead of by group, and any downstream code doesn't care.
    - for each group accumalated:
      - fill in blanks from ldap if neccesary
      - attempt to get quota and consumption by running `lfs quota`
      - returns the group data for the volume
    - transfers the data to the mysql database (`db/report.py`)
        - one of the fields here is `report.warning`, which calculates the status of that group (logic in `lurge_types/group_report.py`).
            - at the start we load all the historical data for each group (`db/__init__.py`)
            - we take the historical data for this group, and calculate the predictions in some days from now
            - this is compared to the values in `directory_config.py`, which are days_from_now:max_percentage pairs for each warning level
            - it then returns the max warning level (the worst and most serious warning)
            - currently, this is:
            <table><tr><th></th><th>Now + 3 Days</th><th>Now + 7 Days</th></tr>
            <tr><th>Usage >95%</th><td>Not OK (3)</td><td>Not OK (3)</td></tr>
            <tr><th>Usage >85%</th><td>Not OK (3)</td><td>Kinda OK (2)</td></tr>
            <tr><th>Usage >80%</th><td> Kinda OK (2) </td><td> - </td></tr>
            </table>
    - writes output to `.tsv` file (`utils/tsv.py`)
    - removes sqlite file

### `project_inspector.py`

This uses MPI, and what happens in each instance is based on its rank. (0..n)

**Rank 0:**
- get groups from ldap (`utils.ldap.py`)
- send information to controllers per volume
- wait for a response from those
- write everything to database
    - First, we're going to load the PI/Group/Volume foreign keys into memory
    - Next, as we're replacing the old data, we're going to tag all the project_names with `.hgi.old.` at the start, instead of deleting it. This'll save us if the additions go wrong
    - For each DirectoryRecord we have, we're going to format the sizes nicely
    - We'll add anything to the foreign tables if neccesary
    - We're then going to add the information to the `directory` MySQL table, and get back the `directory_id`.
    - We can use that ID to then add all the specific filetype data to the `file_size` table.
    - Finally, we can remove any old data - this is data tagged with `.hgi.old`
- Write everything to a TSV file (`utils.tsv.py`)

**Rank <= Number of Volumes:**
- these control all the workers per volume
- read the base directory information
- find the most recent wrstat file for the volume
- iterates over the wrstat file, and sends blocks of 250 records to the workers when they request them
- when done, send a DONE message to all workers, and wait for their response
- collate all the reports from the workers (separate workers could easily have worked on the same directory, so sum up, i.e. file sizes)
- fill in extra information to each report, i.e. group and PI names
- return this to the rank 0 process

**Other Rank:**
- these request work from the controller of the associated volume by sending it their rank number
- when given a block of 250 entries, it'll iterate over each of them
    - it'll find the appropriate base_directory
    - it'll find the subdirectory we'll put the information under
        - if it's a `users` directory, we'll go one level deeper
    - it'll add the relevant information, i.e. filesize, to the DirectoryReport (`lurge_types/directory_report.py`). This is based on whether it is a file or directory
- when it gets a DONE message, it'll send it back to the controller

### `puppeteer.py`

- if not passed volumes to use, uses all volumes
- starts multiprocessing processes for how many wrstats it has to read over
    - finds the most recent wrstat for each volume (`utils/finder.py`)
    - checks that it hasn't already got that data in the DB, otherwise it'll skip that particular wrstat
    - iterates over wrstat file for first time (finding vaults)
        - if it finds the deepest level in a vault (contains `.vault` and is a file), it gets the relative path of the file from the `.vault`
        - using the relative path, and the path of the `.vault`, it can produce the full path
        - it can also get the inode out from the vault path
        - it creats a `VaultPuppet` with the information it has at the moment (`lurge_types/vault.py`)
    - iterates over wrstat file for the second time (finding files affected by vaults)
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

### `user_reporter.py`

- if not passed particular volumes to use, use all volumes
- starts multiprocessing pool for how many wrstats it has to read over
    - finds the most recent wrstat for each volume (`utils/finder.py`)
    - creates a defaultdict of `UserReport` objects (`lurge_types/user.py`) for keeping the information
    - iterates over wrstat file
        - splits up the line information, extracts the user and group (indexes 2 and 3)
        - it adds the size to the `UserReport` objects current size
        - it passes the last modified time to `UserReport`, which'll update the one it stores if its more recent. this means this will end up being the most recent mtime
        - within a `UserReport` object, the size and mtimes are actually stored as defaultdicts, with the key being the group involved.
- next, it'll get some information from ldap, and turn the list of lists of reports into a dictionary, of volume:list_of_reports
- we'll next grab all the unique user ids, and for each of them, store the username against the uid in `usernames`, and store all the groups the user is part of in a list in `user_groups`, where each value is a tuple, `(group_name, group_id)`.
- now we can add all this information to the database (`db/user_reporter.py`)
    - first, we'll grab all the foreign keys from the database and store them in memory
    - next, we'll loop over each volume:list_of_reports pairing
        - if the volume doesn't have a foreign key - add one
        - we can now loop over every user:`UserReport` pairing
            - if the username isn't in the DB - add it
            - now we can loop over the user's groups
                - if the group isn't in the database (you can see a pattern here) - add it
                - finally, if the group_id is in the user's size dictionary for this particular volume, we can execute an `INSERT` query
- now we can write all this information to a TSV file (`utils/tsv.py`)
    - first, we'll write a header row
    - we'll iterate over all the users we have
        - we'll iterate over every group that user is in
            - we'll write a row for that user/group combination with the size per volume, if the user has data in that volume associated to that group
            - same for a row of last modified dates

### `group_splitter.py`
- creates a connection to LDAP servers and gets the humgen group info (`utils/ldap.py`)
- it then creates a directory for us, tagged with the date, or quits if the directory already exists (data already exists for that date)
- then, we'll create a multiprocessing pool for getting the information from the wrstat reports
    - we'll find the report for each volume (`utils/finder.py`)
    - we'll iterate over the wrstat file, and split each line to get the group id, and create a `GroupSplit` object for it (`lurge_types/splitter.py`)
    - when we add a line to the `GroupSplit` object, it'll store it in memory until there's `MAX_LINES_PER_GROUP_PER_VOLUME` in memory, where it'll then write it to a file
    - these files are under `groups/{date}/`, and is a file per group per volume
- after the pool has closed, we'll flush any remaining lines to their files
- as each file is per volume, we need to combine them into files purely by group. luckily, we can just `cat` gzip files together to get another valid `gzip` file, so we don't need to unzip stuff :)
- we can then create an "index file" based on statistics estimating how long TreeServe will take
- we'll also dump passwd and group info to files
- finally, (if neccesary), we'll upload all of this to S3
