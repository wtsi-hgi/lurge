import sqlite3
import ldap
import typing as T


def getLDAPConnection() -> ldap.LDAPObject:
    con = ldap.initialize("ldap://ldap-ro.internal.sanger.ac.uk:389")
    # Sanger internal LDAP is public so no credentials needed
    con.bind("", "")

    return con


def findGroups(ldap_con: ldap.LDAPObject, tmp_db: sqlite3.Connection) -> None:
    """
    Finds Humgen groups using LDAP and writes the group ID number, group name,
    and corresponding PI's surname to table 'group_table' in tmp_db.

    :param ldap_con: LDAP connection to use for queries
    :param tmp_db: SQLite database connection object to write 'group_table' to
    """

    results: T.List[T.Tuple[T.Any, ...]] = ldap_con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
                                                             ldap.SCOPE_ONELEVEL, "(objectClass=sangerHumgenProjectGroup)",
                                                             ['cn', 'gidNumber', 'sangerProjectPI'])

    db_cursor = tmp_db.cursor()
    # cn = groupName, sangerProjectPI['uid'] = PI's user id
    db_cursor.execute('''CREATE TABLE group_table(gidNumber INTEGER PRIMARY KEY,
        groupName TEXT, PI TEXT)''')
    tmp_db.commit()

    # used to collect every PI uid that needs to be resolved, use of set means
    # that duplicate uids are ignored
    PIuids: T.Set[str] = set()

    for item in results:
        # gidNumber is stored as a byte-encoded string in results
        gidNumber = int(item[1]['gidNumber'][0].decode('UTF-8'))
        groupName = item[1]['cn'][0].decode('UTF-8')

        # not all groups have a PI, KeyError thrown in those cases
        try:
            # string in the form 'uid=[xyz],ou=people,dc=sanger,dc=ac,dc=uk'
            # we want just [xyz]
            PIdn = item[1]['sangerProjectPI'][0].decode('UTF-8')

            # first split = 'uid=[xyz]'
            # second split = '[xyz]'
            PIuid = PIdn.split(',')[0].split('=')[1]
            PIuids.add(PIuid)
        except KeyError:
            # nothing to do except skip the PI related code
            PIuid = None

        db_cursor.execute('''INSERT INTO group_table(gidNumber, groupName, PI)
            VALUES(?,?,?)''', (gidNumber, groupName, PIuid))

    # write all prior INSERTs to table in one transaction
    tmp_db.commit()

    # replaces uids in group_table with full surnames of the corresponding PI
    for uid in PIuids:
        surname_result: T.List[T.Tuple[T.Any, ...]] = ldap_con.search_s("ou=people,dc=sanger,dc=ac,dc=uk",
                                                                        ldap.SCOPE_ONELEVEL, "(uid={})".format(uid), ['sn'])

        surname = surname_result[0][1]['sn'][0].decode('UTF-8')

        db_cursor.execute('''UPDATE group_table SET PI = ? WHERE PI = ?''',
                          (surname, uid))

    # write prior UPDATEs to table, nothing needs returning
    tmp_db.commit()
    print("Created table of Humgen Unix groups.")
