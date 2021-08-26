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

    @property
    def __repr__(self) -> str:
        return str(self.__dict__)


class MPIStatFile:

    def __init__(self, path, inode):
        self.path = path.strip("/")
        self.inode = inode
        self.children: T.Dict[str, MPIStatFile] = {}

    @property
    def path_elems(self):
        return self.path.split("/")

    def __len__(self):
        if self.path_elems == [""]:
            return 0
        return len(self.path_elems)

    def insert_child(self, new_file: "MPIStatFile"):
        current_path_length = len(self)
        if current_path_length + 1 == len(new_file):
            self.children[new_file.path_elems[-1]] = new_file
        else:
            self.children[new_file.path_elems[current_path_length]
                          ].insert_child(new_file)

    @property
    def __dict__(self):
        return {
            "path": self.path,
            "inode": self.inode,
            "children": self.children
        }

    def __repr__(self):
        return str(self.__dict__)

    def find_vaults(self) -> T.Set["MPIStatFile"]:
        vaults: T.Set[MPIStatFile] = set()
        if self.path_elems[-1] == ".vault":
            vaults.add(self)
        for child in self.children.values():
            vaults = vaults.union(MPIStatFile.find_vaults(child))
        return vaults
