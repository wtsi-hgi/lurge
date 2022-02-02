# Lustre Usage Report Utilities
lurge generates a report of Lustre usage for groups on Humgen volumes. Intended to replace [Humgen Lustre Usage-Quota Report](https://gitlab.internal.sanger.ac.uk/hgi/lustre-usage).

Project Inspector collates `stat` data for directories, scanning all Humgen project directories by default. Can print a TSV-formatted table to stdout or put the data directly into a MySQL database.

Group Splitter takes mpistat output files and splits them by the Unix group of the owner, writing each chunk to `split/` in the working directory.

## Dependencies
* mysql-connector-python
* python-ldap

## Getting started
1. Clone the repository
2. Copy `db_config.example.py` to `db_config.py` and enter MySQL database credentials in `db_config.py`
3. If necessary, edit declarations in `directory_config.py` to point at the correct directories
4. If necessary, create a Python3 virtual environment and change command in `cron.sh` to point to it 
5. Run `manager.py {report|inspector|puppeteer|users|splitter}`

See `docs/` for more information
