from directory_config import MAX_LINES_PER_GROUP_PER_VOLUME, REPORT_DIR
import typing as T
import gzip
import datetime


class GroupSplit:
    def __init__(self):
        self.lines: T.List[str] = []
        self.line_count: int = 0
        self.directory_count: int = 0

        self.group_name: T.Optional[str] = None
        self.volume: T.Optional[int] = None

    def flush_lines(self):
        with gzip.open(f"{REPORT_DIR}groups/{datetime.datetime.now().strftime('%Y%m%d')}/{self.group_name}.{self.volume}.dat.gz", "at") as f:
            f.writelines(self.lines)
        self.lines = []

    def add_lines(self, line: str):
        self.lines.append(line)
        if len(self.lines) > MAX_LINES_PER_GROUP_PER_VOLUME:
            self.flush_lines()

    def __add__(self, o: "GroupSplit") -> "GroupSplit":
        gs = GroupSplit()
        gs.directory_count = self.directory_count + o.directory_count
        gs.line_count = self.line_count + o.line_count
        return gs
