# lurge - Lustre Usage Report Generator
Generates a report of Lustre usage for groups on Humgen volumes. Eventually intended to replace [Humgen Lustre Usage-Quota Report](https://gitlab.internal.sanger.ac.uk/hgi/lustre-usage).

## Dependencies
* mysql-connector-python
* python-ldap

## Getting started
1. Clone the repository
2. Enter MySQL database credentials in `report_config.py`
3. If necessary, edit declarations in `reportmanager.sh` to point at the correct directories
4. If necessary, create a Python3 virtual environment and change `PY_ENV` in `reportmanager.sh` to point to it 
5. Run `reportmanager.sh`

