from collections import defaultdict


class DirectoryReport:
    def __init__(self, files, mtime, scratch_disk):
        self.size = 0
        self.filetypes = defaultdict(int)
        self.num_files = files
        self.mtime = mtime
        self.pi = None
        self.group_name = None
        self.scratch_disk = scratch_disk
