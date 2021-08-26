import base64
import datetime
from os import stat
import typing as T

import utils


class VaultPuppet:
    @staticmethod
    def from_mpistat(mpistat_file: "MPIStatFile", state: str) -> "VaultPuppet":
        return VaultPuppet(
            full_path=mpistat_file.path,
            state=state,
            inode=mpistat_file.inode,
            size=mpistat_file.size,
            owner=mpistat_file.owner,
            mtime=mpistat_file.mtime
        )

    def __init__(self, full_path: str, state: str, inode: int, size: int, owner: str, mtime: int):
        self.full_path: str = full_path
        self.state: str = state
        self._inode: int = inode

        self.size: str = utils.humanise(size)
        self.owner: str = owner
        self.mtime: datetime.date = datetime.utcfromtimestamp(mtime).date()

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
    @staticmethod
    def from_mpistat(line_info: T.List[T.Any]):
        """
        mpistat lines
        Index   Item
        0       Filepath (base 64 encoded)
        1       Size (bytes)
        2       Owner (ID)
        ...
        5       Last Modified Time (Unix)
        ...
        8       Inode ID
        ...
        """
        filepath = base64.b64decode(
            line_info[0]).decode("UTF-8", "replace")

        return MPIStatFile(
            path=filepath,
            inode=int(line_info[8]),
            size=int(line_info[1]),
            owner=line_info[2],
            mtime=int(line_info[5])
        )

    @staticmethod
    def find_vaults(root: "MPIStatFile") -> T.Set["MPIStatFile"]:
        vaults: T.Set[MPIStatFile] = set()
        if root.path_elems[-1] == ".vault":
            vaults.add(root)
        for child in root.children.values():
            vaults = vaults.union(MPIStatFile.find_vaults(child))
        return vaults

    @staticmethod
    def find_files(root: "MPIStatFile") -> T.Set["MPIStatFile"]:
        files: T.Set[MPIStatFile] = set()
        if len(root.children) == 0:
            files.add(root)
        for child in root.children.values():
            files = files.union(MPIStatFile.find_files(child))
        return files

    @staticmethod
    def find_by_path(root: "MPIStatFile", path: str) -> "MPIStatFile":
        path = path.strip("/")
        current_path_length = len(root)
        if current_path_length + 1 == len(path.split("/")):
            try:
                return root.children[path.split("/")[-1]]
            except KeyError:
                raise FileNotFoundError
        else:
            try:
                return MPIStatFile.find_by_path(root.children[path.split("/")[current_path_length]], path)
            except IndexError:
                raise FileNotFoundError

    def __init__(self, path, inode, size, owner, mtime):
        self.path = path.strip("/")
        self.inode = inode
        self.size = size
        self.owner = owner
        self.mtime = mtime
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
            "children": self.children
        }

    def __repr__(self):
        return str(self.__dict__)
