import datetime
from db_config import SCHEMA
import logging
import typing as T

from lurge_types.vault import VaultPuppet

import mysql.connector

import db.foreign


def write_to_db(conn, vault_reports: T.List[T.Tuple[int, T.Dict[str, VaultPuppet]]], wrstat_dates: T.Dict[int, datetime.date], logger: logging.Logger) -> None:
    logger.info("Writing results to MySQL database")

    cursor = conn.cursor()

    # First, we'll get all the foreign keys for volumes, groups and actions
    # Any missing groups or volumes can be added later
    # ACTIONS WILL NOT BE ADDED LATER
    # We only care about those already in the DB

    _, groups, volumes, actions, users, _, _ = db.foreign.get_db_foreign_keys(
        conn)

    # Now, we're going to go through all the VaultReports and add each as a DB record
    for volume, reports in vault_reports:
        logger.debug(f"Databasing {volume}")
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
                        f"INSERT INTO {SCHEMA}.unix_group (group_name) VALUES (%s);", (vault.group,))
                    new_id: int = cursor.lastrowid
                    groups[vault.group] = new_id
                    db_group = new_id
            else:
                db_group = None

            if vault.owner is not None:
                try:
                    db_user = users[vault.owner]
                except KeyError:
                    cursor.execute(
                        f"INSERT INTO {SCHEMA}.user (user_name) VALUES (%s);", (vault.owner,))
                    new_id = cursor.lastrowid
                    users[vault.owner] = new_id
                    db_user = new_id
            else:
                db_user = None

            if f"scratch{volume}" not in volumes:
                cursor.execute(
                    f"INSERT INTO {SCHEMA}.volume (scratch_disk) VALUES (%s);", (f"scratch{volume}",))
                new_id = cursor.lastrowid
                volumes[f"scratch{volume}"] = new_id

            # Add new data
            query = f"""INSERT INTO {SCHEMA}.vault (record_date, filepath, group_id, vault_action_id, size, 
            user_id, last_modified, volume_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""

            cursor.execute(query, (
                wrstat_dates[volume],
                vault.full_path,
                db_group,
                actions[vault.state],
                vault._size,
                db_user,
                vault._mtime,
                volumes[f"scratch{volume}"]
            ))

    conn.commit()
    logger.info("Puppeteer data loaded into MySQL")
