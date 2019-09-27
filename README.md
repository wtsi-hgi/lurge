# lurge - Lustre Usage Report Generator
Generates a report of Lustre usage for groups on Humgen volumes. Eventually intended to replace [Humgen Lustre Usage-Quota Report](https://gitlab.internal.sanger.ac.uk/hgi/lustre-usage).

## Dependencies
* mysql.connector 

## Getting started
1. Clone the repository
2. Enter MySQL database credentials in `report_config.py`
3. If necessary, edit declarations in `reportmanager.sh` to point at the correct directories
4. Create a Python3 virtual environment called `lurge_env` in the repo directory (make sure to install dependencies)
5. Run `reportmanager.sh`

