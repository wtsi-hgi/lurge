import datetime
import typing as T

import utils
import utils.ldap


class VaultPuppet:
    def __init__(self, full_path: str, state: str, inode: int):
        self.full_path: str = full_path
        self.state: str = state
        self._inode: int = inode

        self._size: T.Optional[int] = None
        self._owner_id: T.Optional[int] = None
        self.owner: T.Optional[str] = None
        self._mtime: T.Optional[datetime.date] = None

    def just_call_my_name(self, size: int, owner: str, mtime: int):
        self._size = size
        self._owner_id = owner
        self._mtime = datetime.datetime.fromtimestamp(mtime).date()

    def pull_your_strings(self, ldap_conn):
        self.owner = utils.ldap.get_username(ldap_conn, self._owner_id)
        self.state = self.state.capitalize()

    @property
    def size(self) -> str:
        if self._size is not None:
            return utils.humanise(self._size)
        raise ValueError("Size not set")

    @property
    def mtime(self) -> str:
        return self._mtime.strftime("%Y-%m-%d")

    @property
    def __dict__(self) -> T.Dict[str, T.Any]:
        return {
            "full_path": self.full_path,
            "inode": self._inode,
            "size": self.size,
            "state": self.state,
            "owner": self.owner,
            "mtime": self.mtime.strftime("%Y%m%d")
        }

    def __repr__(self) -> str:
        return str(self.__dict__)
