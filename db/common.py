import datetime
from db_config import SCHEMA
import logging
import mysql.connector


def get_sql_connection(config) -> mysql.connector.MySQLConnection:
    # connects to the MySQL server used to store the report data, change the
    # credentials here to point at your desired database
    port = config.PORT if config.PORT is not None else 3306

    db_con = mysql.connector.connect(
        host=config.HOST,
        database=config.DATABASE,
        port=port,
        user=config.USER,
        passwd=config.PASSWORD
    )

    return db_con


def check_date(conn: mysql.connector.MySQLConnection, table: str, date: datetime.date,
               volume: int, logger: logging.Logger, base_dir_usage: bool = False) -> bool:
    # Checks the dates in the DB table to see if the date already has data for
    # that particular volume
    cursor = conn.cursor(buffered=True)
    cursor.execute(
        f"""SELECT DISTINCT record_date FROM {SCHEMA}.{table}
        {f'INNER JOIN {SCHEMA}.base_directory USING (base_directory_id)' if base_dir_usage else ''}
        INNER JOIN {SCHEMA}.volume USING (volume_id)
        WHERE scratch_disk = %s""", (f"scratch{volume}",)
    )

    for (result,) in cursor:
        if date == result:
            logger.warning(f"{volume} already has DB data for {date}")
            return True
    return False
