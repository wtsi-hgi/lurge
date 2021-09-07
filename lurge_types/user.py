from collections import defaultdict
import datetime
import typing as T


class UserReport:
    def __init__(self):
        self.size: T.DefaultDict[str, int] = defaultdict(int)
        self._mtime: T.DefaultDict[str,
                                   datetime.date] = defaultdict(datetime.date)

    def mtime(self, t, grp):
        new_date = datetime.datetime.fromtimestamp(t).date()
        if new_date > self._mtime[grp]:
            self._mtime[grp] = new_date

    def __str__(self) -> str:
        return str({
            "size": self.size,
            "mtime": self._mtime
        })
