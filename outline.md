* `report_cron.sh`
  * bsub: `reportmanager.sh`
    * tries to determine latest full set of mpistat (buggy)
    * only looks over the past N days
    * won't proceed if report-DATE.tsv exists (buggy)
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
  * bsub: inspector_manager.sh

* `group_splitter_cron.sh`
  * bsub: group_splitter_manager.sh
