import datetime
from directory_config import DEFAULT_WARNING, WARNINGS, WRSTAT_DIR
import typing as T

import utils.finder

from db import historical_usage
from . import ReportIdentifier


class GroupReport:
    @staticmethod
    def create_non_humgen(gid, volume, usage, last_modified):
        gr = GroupReport(gid, None, None, volume)
        gr.usage = usage
        gr.last_modified = last_modified
        return gr

    def __init__(self, gid: str, group_name: T.Optional[str], pi_name: T.Optional[str], volume: str):
        self.gid: str = gid
        self.group_name: T.Optional[str] = group_name
        self.pi_name: T.Optional[str] = pi_name
        self.usage: int = 0
        self.quota: int = None
        self.last_modified: int = 0
        self.volume: str = volume
        self.isHumgen: bool = True
        self.archived_dirs: T.Optional[str] = None

        latest_wr = utils.finder.findReport(
            volume, WRSTAT_DIR)
        wr_date_str = latest_wr.split("/")[-1].split("_")[0]
        self._date: datetime.date = datetime.date(int(wr_date_str[:4]), int(
            wr_date_str[4:6]), int(wr_date_str[6:8]))

    def calculate_last_modified_rel(self, wrstat_time):
        self.last_modified_rel = max(0, (wrstat_time - self.last_modified) // 86400)

    @property
    def row(self):
        return [self.volume, self.pi_name, self.group_name, self.usage, self.quota, self.last_modified_rel, self.archived_dirs]

    @property
    def id(self):
        return ReportIdentifier(self.group_name, self.pi_name, self.volume)

    @property
    def warning(self) -> T.Optional[int]:
        def _prediction(history, days_from_now) -> int:
            points = min(len(history), 2)
            if points == 0:
                return self.usage
            else:
                delta_past_1 = (
                    datetime.datetime.today().date() - self._date).days
                delta_past_2 = (datetime.datetime.today(
                ).date() - history[-points][0]).days

                prediction = self.usage + ((days_from_now + delta_past_1)/(
                    delta_past_2 - delta_past_1)) * (self.usage - history[-points][1])
                return prediction

        history = historical_usage[self.id]

        prediction = max([DEFAULT_WARNING, *[level for level, criteria in WARNINGS.items() if True in map(
            lambda x: _prediction(history, x[0])/self.quota > x[1] if self.quota is not None and self.quota > 0 else 0, criteria)]])

        return prediction
