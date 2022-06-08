from collections import defaultdict
import typing as T

class DirectoryReport:
    def __init__(self, files: int, mtime: int, scratch_disk: int):
        self.size: int = 0
        self.filetypes: T.DefaultDict[str, int] = defaultdict(int)
        self.num_files: int = files
        self.mtime: int = mtime
        self.scratch_disk: int = scratch_disk

        self.base_path: T.Optional[str] = None
        self.subdir: T.Optional[str] = None

        self.pi: T.Optional[str] = None
        self.group_name: T.Optional[str] = None
        self.relative_mtime: float = 0
