import typing as T

from lurge_types.vault import VaultPuppet


def write_to_db(conn, vault_reports: T.Dict[int, T.Dict[str, VaultPuppet]]) -> None:
    print("Writing results to MySQL database")
    
    # TODO
    ...