import datetime
import typing as T

import utils


class VaultPuppet:
    def __init__(self, full_path: str, state: str, inode: int):
        self.full_path: str = full_path
        self.state: str = state
        self._inode: int = inode

        self.size: T.Optional[str] = None
        self.owner: T.Optional[int] = None
        self.mtime: T.Optional[datetime.date] = None

    def just_call_my_name(self, size: int, owner: str, mtime: int):
        self.size = utils.humanise(size)
        self.owner = owner
        self.mtime = datetime.utcfromtimestamp(mtime).date()

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
