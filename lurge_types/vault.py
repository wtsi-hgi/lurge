import datetime
import typing as T

import utils
import utils.ldap

from utils.symlink import get_mdt_symlink


class VaultPuppet:
    def __init__(self, full_path: str, state: str, inode: int):
        self.full_path: str = full_path
        self.state: str = state
        self._inode: int = inode

        self._size: T.Optional[int] = None
        self._owner_id: T.Optional[int] = None
        self.owner: T.Optional[str] = None
        self._group_id: T.Optional[str] = None
        self.group: T.Optional[str] = None
        self._mtime: T.Optional[datetime.date] = None

    def just_call_my_name(self, size: int, owner: str, mtime: int, group_id: int):
        self._size = size
        self._owner_id = owner
        self._mtime = datetime.datetime.fromtimestamp(mtime).date()
        self._group_id = group_id

    def pull_your_strings(self, ldap_conn, groups):
        self.owner = utils.ldap.get_username(ldap_conn, self._owner_id)
        self.state = self.state.capitalize()

        try:
            self.group = groups[self._group_id] if self._group_id is not None else None
        except KeyError:
            self.group = None

        self.full_path = get_mdt_symlink(self.full_path)

    @property
    def size(self) -> str:
        if self._size is not None:
            return utils.humanise(self._size)
        raise ValueError("Size not set")

    @property
    def mtime(self) -> str:
        return self._mtime.strftime("%Y-%m-%d")

    def __repr__(self) -> str:
        return str({
            "full_path": self.full_path,
            "size": self.size,
            "state": self.state,
            "owner": self.owner,
            "mtime": self.mtime,
            "group": self.group
        })
