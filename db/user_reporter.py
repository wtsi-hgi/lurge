import datetime
import logging
from lurge_types.user import UserReport
import mysql.connector
import typing as T


def load_user_reports_to_db(conn, volume_user_reports: T.Dict[int, T.DefaultDict[str, UserReport]], usernames: T.Dict[int, str], user_groups: T.Dict[str, T.List[T.Tuple[str, str]]], logger: logging.Logger):
    logger.info("Writing results to MySQL database")

    cursor = conn.cursor()

    # First, we'll get all the foreign keys

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

    # Users
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.user")
    user_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    users: T.Dict[str, int] = {}
    for user_id, user_name in user_results:
        users[user_name] = user_id

    # Now, we'll go through every record (just like in the TSV generator) and
    # add a DB record for all of them

    for volume, reports in volume_user_reports.items():
        logger.debug(f"Databasing {volume}")

        if f"scratch{volume}" not in volumes:
            cursor.execute(
                "INSERT INTO hgi_lustre_usage_new.volume (scratch_disk) VALUES (%s);", (f"scratch{volume}",))
            new_id = cursor.lastrowid
            volumes[f"scratch{volume}"] = new_id
            conn.commit()

        for uid, report in reports.items():

            if uid is not None:
                try:
                    db_user = users[usernames[int(uid)]]
                except KeyError:
                    cursor.execute(
                        "INSERT INTO hgi_lustre_usage_new.user (user_name) VALUES (%s);", (usernames[int(uid)],))
                    new_id = cursor.lastrowid
                    conn.commit()
                    users[usernames[int(uid)]] = new_id
                    db_user = new_id
            else:
                db_user = None

            for grp_name, gid in user_groups[uid]:
                if grp_name is not "-":
                    try:
                        db_group = groups[grp_name]
                    except KeyError:
                        cursor.execute(
                            "INSERT INTO hgi_lustre_usage_new.unix_group (group_name, is_humgen) VALUES (%s, %s);", (grp_name, 1))
                        new_id = cursor.lastrowid
                        conn.commit()
                        groups[grp_name] = new_id
                        db_group = new_id
                else:
                    db_group = None

                if gid in report.size:
                    cursor.execute("INxSERT INTO hgi_lustre_usage_new.user_usage (record_date, user_id, group_id, volume_id, size, last_modified) VALUES (%s, %s, %s, %s, %s, %s);", (
                        datetime.datetime.today().date(),
                        db_user,
                        db_group,
                        volumes[f"scratch{volume}"],
                        report.size[gid],
                        report._mtime[gid]
                    ))
    conn.commit()

    logger.info("Finished writing user reports to DB")
