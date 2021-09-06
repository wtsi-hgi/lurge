import datetime
import typing as T


class UserReport:
    def __init__(self):
        self.size: int = 0
        self._mtime: T.Optional[datetime.date] = None

    def mtime(self, t):
        new_date = datetime.datetime.fromtimestamp(t).date
        if self._mtime is None or new_date > self._mtime:
            self._mtime = new_date

    def __str__(self) -> str:
        return str({
            "size": self.size,
            "mtime": self._mtime
        })
