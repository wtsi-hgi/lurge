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
      * TODO...

* `group_splitter_cron.sh`
  * bsub: `group_splitter_manager.sh`
    * TODO...

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
