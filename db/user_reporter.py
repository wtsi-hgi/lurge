import datetime
import logging
import typing as T

import db.foreign
from db_config import SCHEMA
from lurge_types.user import UserReport


def load_user_reports_to_db(
    conn,
    volume_user_reports: T.Dict[int, T.DefaultDict[str, UserReport]],
    usernames: T.Dict[int, str],
    user_groups: T.Dict[str, T.Set[T.Tuple[str, str]]],
    wrstat_dates: T.Dict[int, datetime.date],
    logger: logging.Logger
):
    logger.info("Writing results to MySQL database")

    cursor = conn.cursor()

    # First, we'll get all the foreign keys

    _, groups, volumes, _, users, _, _ = db.foreign.get_db_foreign_keys(conn)

    # Now, we'll go through every record (just like in the TSV generator) and
    # add a DB record for all of them

    for volume, reports in volume_user_reports.items():
        logger.debug(f"Databasing {volume}")

        if f"scratch{volume}" not in volumes:
            cursor.execute(
                f"INSERT INTO {SCHEMA}.volume (scratch_disk) VALUES (%s);", (f"scratch{volume}",))
            new_id = cursor.lastrowid
            volumes[f"scratch{volume}"] = new_id
            conn.commit()

        for uid, report in reports.items():

            if uid is not None:
                try:
                    db_user = users[usernames[int(uid)]]
                except KeyError:
                    cursor.execute(
                        f"INSERT INTO {SCHEMA}.user (user_name) VALUES (%s);", (usernames[int(uid)],))
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
                            f"INSERT INTO {SCHEMA}.unix_group (group_name) VALUES (%s);", (grp_name,))
                        new_id: int = cursor.lastrowid
                        conn.commit()
                        groups[grp_name] = new_id
                        db_group = new_id
                else:
                    db_group = None

                if gid in report.size:
                    cursor.execute(f"INSERT INTO {SCHEMA}.user_usage (record_date, user_id, group_id, volume_id, size, last_modified) VALUES (%s, %s, %s, %s, %s, %s);", (
                        wrstat_dates[volume],
                        db_user,
                        db_group,
                        volumes[f"scratch{volume}"],
                        report.size[gid],
                        report._mtime[gid]
                    ))
    conn.commit()

    logger.info("Finished writing user reports to DB")
