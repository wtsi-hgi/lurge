import datetime
from directory_config import DEFAULT_WARNING, WARNINGS
import typing as T

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

    def calculate_last_modified_rel(self, wrstat_time):
        self.last_modified_rel = (wrstat_time - self.last_modified) // 86400

    @property
    def row(self):
        return [self.volume, self.pi_name, self.group_name, self.usage, self.quota, self.last_modified_rel, self.archived_dirs]

    @property
    def id(self):
        return ReportIdentifier(self.group_name, self.pi_name, self.volume)

    @property
    def warning(self) -> T.Optional[int]:
        def _prediction(history, days_from_now) -> int:
            points = min(len(history), 3)
            if points == 1:
                return history[0][1]
            else:
                delta_past_1 = (
                    datetime.datetime.today().date() - history[-1][0]).days
                delta_past_2 = (datetime.datetime.today(
                ).date() - history[-points][0]).days

                prediction = history[-1][0] + ((days_from_now + delta_past_1)/(
                    delta_past_2 - delta_past_1)) * (history[-1][1] - history[-points][1])
                return prediction

        history = historical_usage[self.id]
        if history == []: return None

        prediction = max([DEFAULT_WARNING, *[level for level, criteria in WARNINGS.items() if True in map(
            lambda x: _prediction(history, x[0])/self.quota > x[1] if self.quota > 0 else 0, criteria)]])

        return prediction
