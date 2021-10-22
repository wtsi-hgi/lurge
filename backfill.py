"""
Backfilling many days of records. See backfill.sh
"""

import sys

import report

report.main(int(sys.argv[1]))
