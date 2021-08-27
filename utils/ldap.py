import sqlite3
import ldap
import typing as T


def getLDAPConnection():
    con = ldap.initialize("ldap://ldap-ro.internal.sanger.ac.uk:389")
    # Sanger internal LDAP is public so no credentials needed
    con.bind("", "")

    return con


def get_humgen_ldap_info(ldap_con) -> T.Tuple[T.Dict[str, str], T.Dict[str, str]]:
    # Ask the Sanger LDAP for Humgen Groups
    results: T.List[T.Tuple[T.Any, ...]] = ldap_con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
                                                             ldap.SCOPE_ONELEVEL, "(objectClass=sangerHumgenProjectGroup)",
                                                             ["gidNumber", "sangerProjectPI", "cn"])

    PIuids: T.Set[str] = set()
    group_pis: T.Dict[str, str] = {}
    group_names: T.Dict[str, str] = {}

    # Put the group names and PIs into maps with the group ID
    for entry in results:
        gidNumber = entry[1]["gidNumber"][0].decode("UTF-8", "replace")
        group_name = entry[1]["cn"][0].decode("UTF-8", "replace")

        try:
            PIdn = entry[1]['sangerProjectPI'][0].decode('UTF-8')
            PIuid = PIdn.split(',')[0].split('=')[1]
            PIuids.add(PIuid)
        except KeyError:
            PIuid = None

        group_names[gidNumber] = group_name
        group_pis[gidNumber] = PIuid

    pi_sn: T.Dict[str, str] = {}

    # Ask the Sanger LDAP for PI surnames
    for PIuid in PIuids:
        surname_result = ldap_con.search_s(
            "ou=people,dc=sanger,dc=ac,dc=uk", ldap.SCOPE_ONELEVEL, f"(uid={PIuid})", ["sn"])

        surname = surname_result[0][1]["sn"][0].decode("UTF-8")
        pi_sn[PIuid] = surname

    # Replace PI IDs with names
    for gid in group_pis:
        if group_pis[gid] is not None:
            group_pis[gid] = pi_sn[group_pis[gid]]

    return (group_pis, group_names)


def add_humgen_ldap_to_db(ldap_con, tmp_db: sqlite3.Connection) -> None:
    # Add all the LDAP information to the temporary sqlite database
    db_cursor = tmp_db.cursor()
    db_cursor.execute(
        "CREATE TABLE group_table(gidNumber INTEGER PRIMARY KEY, groupName TEXT, PI TEXT);")
    tmp_db.commit()

    pis, groups = get_humgen_ldap_info(ldap_con)

    for gid in groups:
        db_cursor.execute(
            "INSERT INTO group_table (gidNumber, groupName, PI) VALUES (?, ?, ?)", (gid, groups[gid], pis[gid]))
    tmp_db.commit()


def get_username(ldap_conn, uid: int) -> str:
    result = ldap_conn.search_s(
        "ou=people,dc=sanger,dc=ac,dc=uk", ldap.SCOPE_ONELEVEL, f"(uidNumber={uid})", ["uid"])

    return result[0][1]["uid"][0].decode("UTF-8")
