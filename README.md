# Lurge
Lustre Usage Report GEnerator _(i presume)_

---

Lurge summarises information about Lustre usage for groups. Creates data for **Weaver** [(GitHub)](https://github.com/wtsi-hgi/weaver), [(App)](https://apps.hgi.sanger.ac.uk/weaver). Originally intended to replace [Humgen Lustre Usage-Quota Report](https://gitlab.internal.sanger.ac.uk).

The primary part of this is `group_reporter.py`, summarises info from `wrstat` for groups, directories. It puts everything in a MySQL database, and can output a TSV file of the information.

## Dependencies
For Python dependencies, see `requirements.txt`. `group_reporter.py` also requires MPI to run, see [Running MPI Jobs on the Sanger Compute Farm](https://ssg-confluence.internal.sanger.ac.uk/pages/viewpage.action?pageId=101361162).

The file `cron.sh` will help, as that is how we run it daily.

## Getting Started
- Copy `db_config.example.py` to `db_config.py` and put the MySQL database credentials in here. Consider setting the permissions to this file to `go-r` as appropriate.
- If neccesary, edit the configuration in `directory_config.py`.
- It is recommended you create an environment just for Lurge, using `python3 -m venv .venv`, `source .venv/bin/activate` and `pip install -r requirements.txt` as needed.
- Use `mpirun` to run the `group_reporter.py` script, or `manager.py` to run the others.

See `docs/` for more information.