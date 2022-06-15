import mysql.connector
from collections import defaultdict
from db_config import SCHEMA
import typing as T
import datetime

History = T.DefaultDict[T.Tuple[T.Optional[str],
                                T.Optional[str]], T.List[T.Tuple[datetime.date, int]]]


def get_all_historical_usage_data(conn: mysql.connector.MySQLConnection) -> History:
    all_history: History = defaultdict(list)

    cursor = conn.cursor()
    cursor.execute(f"""
    SELECT used, record_date, group_name, directory_path 
    FROM {SCHEMA}.lustre_usage
    INNER JOIN {SCHEMA}.unix_group ug on lustre_usage.unix_id = ug.group_id
    INNER JOIN {SCHEMA}.base_directory USING (base_directory_id)    
    ORDER BY record_date ASC 
    """)

    history_results: T.Iterable[T.Tuple[int,
                                        datetime.date, str, str]] = cursor.fetchall()
    for usage, date, group, path in history_results:
        all_history[(group, path)].append((date, usage))

    return all_history
