import typing as T

import ldap
import ldap.ldapobject


def get_ldap_connection() -> ldap.ldapobject.LDAPObject:
    con: ldap.ldapobject.LDAPObject = ldap.initialize(
        "ldap://ldap-ro.internal.sanger.ac.uk:389")
    # Sanger internal LDAP is public so no credentials needed
    con.bind("", "")

    return con


def get_groups_ldap_info(
        ldap_con: ldap.ldapobject.LDAPObject) -> T.Tuple[T.Dict[int, str], T.Dict[int, str]]:
    # Ask the Sanger LDAP for Humgen Groups
    results: T.List[T.Tuple[T.Any, ...]] = ldap_con.search_s("ou=group,dc=sanger,dc=ac,dc=uk",
                                                             ldap.SCOPE_ONELEVEL, "(objectClass=*)",
                                                             ["gidNumber", "sangerProjectPI", "cn"])

    PIuids: T.Set[str] = set()
    group_pis: T.Dict[int, str] = {}
    group_names: T.Dict[int, str] = {}

    # Put the group names and PIs into maps with the group ID
    for entry in results:
        gidNumber = int(entry[1]["gidNumber"][0].decode("UTF-8", "replace"))
        group_name = entry[1]["cn"][0].decode("UTF-8", "replace")

        try:
            PIdn = entry[1]['sangerProjectPI'][0].decode('UTF-8')
            PIuid = PIdn.split(',')[0].split('=')[1]
            PIuids.add(PIuid)
        except KeyError:
            PIuid = ""

        group_names[gidNumber] = group_name
        group_pis[gidNumber] = PIuid

    pi_sn: T.Dict[str, str] = {}

    # Ask the Sanger LDAP for PI surnames
    for PIuid in PIuids:
        surname_result = ldap_con.search_s(
            "ou=people,dc=sanger,dc=ac,dc=uk", ldap.SCOPE_ONELEVEL, f"(uid={PIuid})", ["sn"])

        surname: str = surname_result[0][1]["sn"][0].decode("UTF-8")
        pi_sn[PIuid] = surname

    # Replace PI IDs with names
    for gid in group_pis:
        if group_pis[gid]:
            group_pis[gid] = pi_sn[group_pis[gid]]

    return (group_pis, group_names)


def get_username(ldap_conn: ldap.ldapobject.LDAPObject, uid: int) -> str:
    result = ldap_conn.search_s(
        "ou=people,dc=sanger,dc=ac,dc=uk", ldap.SCOPE_ONELEVEL, f"(uidNumber={uid})", ["uid"])

    try:
        return result[0][1]["uid"][0].decode("UTF-8")
    except IndexError:
        return ""
