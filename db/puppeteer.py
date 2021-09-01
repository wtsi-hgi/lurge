import datetime
import typing as T

from lurge_types.vault import VaultPuppet

import mysql.connector


def check_report_date(db_conn: mysql.connector.MySQLConnection, date: datetime.date, volume: int):
    """
    Checks the dates in the MySQL database to see if date 'date'
    is already recorded.

    :param sql_db: MySQL connection to check for reports
    :param date: The date of the report to be produced
    """
    sql_cursor = db_conn.cursor(buffered=True)
    sql_cursor.execute(
        """SELECT DISTINCT record_date FROM hgi_lustre_usage_new.vault
        INNER JOIN hgi_lustre_usage_new.volume USING (volume_id)
        WHERE scratch_disk = %s""", (f"scratch{volume}",))

    for (result,) in sql_cursor:
        if (date == result):
            print(f"{volume} already has DB data for {date}")
            return True
    return False


def write_to_db(conn, vault_reports: T.List[T.Tuple[int, T.Dict[str, VaultPuppet]]]) -> None:
    print("Writing results to MySQL database")

    cursor = conn.cursor()

    # First, we'll get all the foreign keys for volumes, groups and actions
    # Any missing groups or volumes can be added later
    # ACTIONS WILL NOT BE ADDED LATER
    # We only care about those already in the DB

    # Groups
    # We don't care if they're part of HumGen or not, so we'll just assume they are
    cursor.execute(
        "SELECT group_id, group_name FROM hgi_lustre_usage_new.unix_group WHERE is_humgen = 1")
    group_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    groups: T.Dict[str, int] = {}
    for group_id, group_name in group_results:
        groups[group_name] = group_id

    # Volumes
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.volume")
    volume_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    volumes: T.Dict[str, int] = {}
    for volume_id, volume_name in volume_results:
        volumes[volume_name] = volume_id

    # Vault Actions
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.vault_actions")
    action_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    actions: T.Dict[str, int] = {}
    for action_id, action_name in action_results:
        actions[action_name] = action_id

    # Now, we're going to go through all the VaultReports and add each as a DB record
    for volume, reports in vault_reports:
        print(f"Databasing {volume}")
        for vault in reports.values():
            # Add foreign keys if they don't exist
            # We're NOT adding more Action items
            # If the action isn't in the DB, we skip the vault

            if vault.state not in actions:
                continue

            if vault.group is not None:
                try:
                    db_group = groups[vault.group]
                except KeyError:
                    cursor.execute(
                        "INSERT INTO hgi_lustre_usage_new.unix_group (group_name, is_humgen) VALUES (%s, %s);", (vault.group, 1))
                    (new_id,) = cursor.lastrowid
                    groups[vault.group] = new_id
            else:
                db_group = None

            if f"scratch{volume}" not in volumes:
                cursor.execute(
                    "INSERT INTO hgi_lustre_usage_new.volume (scratch_disk) VALUES (%s);", (f"scratch{volume}",))
                (new_id,) = cursor.lastrowid
                volumes[f"scratch{volume}"] = new_id

            # Add new data
            query = """INSERT INTO hgi_lustre_usage_new.vault (record_date, filepath, group_id, vault_action_id, size, 
            file_owner, last_modified, volume_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

            cursor.execute(query, (
                datetime.datetime.now().date(),
                vault.full_path,
                db_group,
                actions[vault.state],
                vault._size,
                vault.owner,
                vault._mtime,
                volumes[f"scratch{volume}"]
            ))

    conn.commit()
    print("Puppeteer data loaded into MySQL")
