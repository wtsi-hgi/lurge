import datetime
import typing as T
from collections import defaultdict


def _datetime_constructor():
    return datetime.date(1970, 1, 1)


class UserReport:
    def __init__(self):
        self.size: T.DefaultDict[str, int] = defaultdict(int)
        self._mtime: T.DefaultDict[str,
                                   datetime.date] = defaultdict(_datetime_constructor)

    def mtime(self, t, grp):
        new_date = datetime.datetime.fromtimestamp(t).date()
        if new_date > self._mtime[grp]:
            self._mtime[grp] = new_date

    def __str__(self) -> str:
        return str({
            "size": self.size,
            "mtime": self._mtime
        })
