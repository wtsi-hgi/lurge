import typing as T


class GroupSplit:
    def __init__(self):
        self.lines: T.Set[str] = set()
        self.directory_count: int = 0
        self.group_name: T.Optional[str] = None

    def __add__(self, o: "GroupSplit") -> "GroupSplit":
        gs = GroupSplit()
        gs.lines = self.lines.union(o.lines)
        gs.directory_count = self.directory_count + o.directory_count
        return gs
