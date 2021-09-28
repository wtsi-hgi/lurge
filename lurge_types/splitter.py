from directory_config import MAX_LINES_PER_GROUP_PER_VOLUME, REPORT_DIR
import typing as T
import gzip
import datetime


class GroupSplit:
    def __init__(self, volume: int):
        self.lines: T.List[str] = []
        self.directory_count: int = 0
        self.group_name: T.Optional[str] = None
        self.volume: int = volume

    def flush_lines(self):
        with gzip.open(f"{REPORT_DIR}/groups/{datetime.datetime.now().strftime('%Y%m%d')}/{self.group_name}.{self.volume}.dat.gz", "at") as f:
            f.writelines(self.lines)

    @lines.setter
    def lines(self, line: str):
        self.lines.append(line)
        if len(self.lines) > MAX_LINES_PER_GROUP_PER_VOLUME:
            self.flush_lines()
            self.lines = []

    def __add__(self, o: "GroupSplit") -> "GroupSplit":
        gs = GroupSplit()
        gs.lines = self.lines + o.lines
        gs.directory_count = self.directory_count + o.directory_count
        return gs
