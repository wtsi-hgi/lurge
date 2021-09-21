import mysql.connector
from collections import defaultdict
from db_config import SCHEMA
from lurge_types import ReportIdentifier
import typing as T
import datetime


def get_all_historical_usage_data(conn: mysql.connector.MySQLConnection) -> T.DefaultDict[ReportIdentifier, T.List[T.Tuple[datetime.date, int]]]:
    all_history = defaultdict(list)

    cursor = conn.cursor()
    cursor.execute(f"""
    SELECT used, record_date, group_name, pi_name, scratch_disk 
    FROM {SCHEMA}.lustre_usage
    INNER JOIN unix_group ug on lustre_usage.unix_id = ug.group_id
    INNER JOIN pi USING (pi_id)
    INNER JOIN volume USING (volume_id)
    ORDER BY record_date ASC 
    """)

    history_results = cursor.fetchall()
    for usage, date, group, pi, volume in history_results:
        all_history[ReportIdentifier(group, pi, volume)].append((date, usage))

    return all_history
