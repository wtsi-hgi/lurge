from collections import defaultdict
import mysql.connector
import typing as T


def get_db_foreign_keys(db_conn: mysql.connector.MySQLConnection, humgen_only: bool = True) -> T.Tuple[
    T.Dict[str, int],
    T.Union[T.Dict[str, int], T.DefaultDict[str, T.Dict[int, int]]],
    T.Dict[str, int],
    T.Dict[str, int],
    T.Dict[str, int]
]:
    """Get all the foreign keys from the database

    :param db_conn: - The connection to the MySQL database
    :param humgen_only: - bool - whether we want only human genetics groups

    :returns: PIs, Groups, Volumes, Vault Actions, Users
    """

    cursor = db_conn.cursor()

    # PIs
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.pi")
    pi_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    pis: T.Dict[str, int] = {}
    for (pi_id, pi_name) in pi_results:
        pis[pi_name] = pi_id

    # Groups
    if humgen_only:
        cursor.execute(
            "SELECT group_id, group_name FROM hgi_lustre_usage_new.unix_group WHERE is_humgen = 1")
        group_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

        groups: T.Dict[str, int] = {}
        for (group_id, group_name) in group_results:
            groups[group_name] = group_id
    else:
        cursor.execute("SELECT * FROM hgi_lustre_usage_new.unix_group")
        group_results: T.List[T.Tuple[int, str, int]] = cursor.fetchall()

        groups: T.DefaultDict[str, dict[int, int]] = defaultdict(dict)
        for (group_id, group_name, isHumgen) in group_results:
            groups[group_name][isHumgen] = group_id

    # Volumes
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.volume")
    volume_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    volumes: T.Dict[str, int] = {}
    for (volume_id, volume_name) in volume_results:
        volumes[volume_name] = volume_id

    # Vault Actions
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.vault_actions")
    action_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    actions: T.Dict[str, int] = {}
    for action_id, action_name in action_results:
        actions[action_name] = action_id

    # Users
    cursor.execute("SELECT * FROM hgi_lustre_usage_new.user")
    user_results: T.List[T.Tuple[int, str]] = cursor.fetchall()

    users: T.Dict[str, int] = {}
    for user_id, user_name in user_results:
        users[user_name] = user_id

    return pis, groups, volumes, actions, users
