from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
import datetime
from directory_config import DEFAULT_WARNING, WARNINGS, WRSTAT_DIR
import typing as T

import utils.finder

from db import historical_usage

@dataclass
class NewDirectoryReport:
    mtime: int
  
    size: int = 0
    num_files: int = 0
    filetypes: T.DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))

    wrstat_time: int = int(datetime.datetime.now().timestamp())

    @property
    def relative_mtime(self) -> float:
        return max(0, round((self.wrstat_time - self.mtime) / 86400, 1))
    

@dataclass
class NewGroupReport:
    # gid: int
    volume: int

    group_name: T.Optional[str] = None
    pi_name: T.Optional[str] = None
    base_path: T.Optional[str] = None

    usage: int = 0
    last_modified: int = 0
    quota: T.Optional[int] = None

    subdirs: T.Dict[str, NewDirectoryReport] = field(default_factory=lambda: {})

    _wrstat_time: int = int(datetime.datetime.now().timestamp())

    # TODO: the rest

    def __iadd__(self, o: NewGroupReport):
        self.usage += o.usage
        self.last_modified = max(self.last_modified, o.last_modified)
        
        for subdir, subdir_report in o.subdirs.items():
            if subdir not in self.subdirs:
                self.subdirs[subdir] = subdir_report
            else:
                self.subdirs[subdir].size += subdir_report.size
                self.subdirs[subdir].num_files += subdir_report.num_files
                self.subdirs[subdir].mtime = max(self.subdirs[subdir].mtime, subdir_report.mtime)

                for key, value in subdir_report.filetypes.items():
                    self.subdirs[subdir].filetypes[key] += value

        return self
    
    @property
    def relative_mtime(self) -> float:
        return max(0, round((self.wrstat_time - self.last_modified) / 86400, 1))

    @property
    def warning(self) -> T.Optional[int]:
        def _prediction(history: T.List[T.Tuple[datetime.date, int]], days_from_now: int) -> float:
            points = min(len(history), 2)
            if points == 0:
                return self.usage
            else:
                delta_past_1 = (
                    datetime.datetime.today() - datetime.datetime.fromtimestamp(self.wrstat_time)).days
                delta_past_2 = (datetime.datetime.today(
                ).date() - history[-points][0]).days

                prediction: float = self.usage + ((days_from_now + delta_past_1)/(
                    delta_past_2 - delta_past_1)) * (self.usage - history[-points][1])
                return prediction

        history = historical_usage[(self.group_name, self.base_path)]

        prediction = max([DEFAULT_WARNING, *[level for level, criteria in WARNINGS.items() if True in map(
            lambda x: _prediction(history, x[0])/self.quota > x[1] if self.quota is not None and self.quota > 0 else 0, criteria)]])

        return prediction

    @property
    def wrstat_time(self) -> int:
        return self._wrstat_time

    @wrstat_time.setter
    def wrstat_time(self, time: int) -> None:
        self._wrstat_time = time
        for subdir_report in self.subdirs.values():
            subdir_report.wrstat_time = time



class GroupReport:

    def __init__(self, gid: str, path: str, group_name: T.Optional[str], pi_name: T.Optional[str], volume: str):
        self.gid: str = gid
        self.base_path: str = path
        self.group_name: T.Optional[str] = group_name
        self.pi_name: T.Optional[str] = pi_name
        self.usage: int = 0
        self.quota: T.Optional[int] = None
        self.last_modified: int = 0
        self._volume: str = volume

        latest_wr = utils.finder.find_report(
            volume, WRSTAT_DIR) or ""
        wr_date_str = latest_wr.split("/")[-1].split("_")[0]
        self._date: datetime.date = datetime.date(int(wr_date_str[:4]), int(
            wr_date_str[4:6]), int(wr_date_str[6:8]))

    def calculate_last_modified_rel(self, wrstat_time: int) -> None:
        self.last_modified_rel = max(
            0, (wrstat_time - self.last_modified) // 86400)

    row_headers: T.List[str] = [
        "Top Level Path",
        "PI",
        "Group",
        "Usage (bytes)",
        "Quota (bytes)",
        "Last Modified (days)"
    ]

    @property
    def row(self) -> T.List[T.Union[str, int, None]]:
        return [
            self.base_path,
            self.pi_name,
            self.group_name,
            self.usage,
            self.quota,
            self.last_modified_rel
        ]

    @property
    def warning(self) -> T.Optional[int]:
        def _prediction(history: T.List[T.Tuple[datetime.date, int]], days_from_now: int) -> float:
            points = min(len(history), 2)
            if points == 0:
                return self.usage
            else:
                delta_past_1 = (
                    datetime.datetime.today().date() - self._date).days
                delta_past_2 = (datetime.datetime.today(
                ).date() - history[-points][0]).days

                prediction: float = self.usage + ((days_from_now + delta_past_1)/(
                    delta_past_2 - delta_past_1)) * (self.usage - history[-points][1])
                return prediction

        history = historical_usage[(self.group_name, self.base_path)]

        prediction = max([DEFAULT_WARNING, *[level for level, criteria in WARNINGS.items() if True in map(
            lambda x: _prediction(history, x[0])/self.quota > x[1] if self.quota is not None and self.quota > 0 else 0, criteria)]])

        return prediction
