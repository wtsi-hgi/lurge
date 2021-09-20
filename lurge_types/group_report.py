from directory_config import DEFAULT_WARNING, WARNINGS
import typing as T

from . import historical_usage


class ReportIdentifier:
    def __init__(self, group, pi, volume):
        self.group = group
        self.pi = pi
        self.volume = volume

    def __hash__(self) -> int:
        return hash((self.group, self.pi, self.volume))

    def __eq__(self, o: "ReportIdentifier") -> bool:
        return self.group == o.group and self.pi == o.pi and self.volume == o.volume


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
    def warning(self) -> int:
        def _prediction(history, days_from_now) -> int:
            # TODO Prediction Function
            ...

        history = historical_usage[self.id]

        prediction = max([DEFAULT_WARNING, *[level for level, criteria in WARNINGS if True in map(
            lambda x, y: _prediction(history, x) > y, criteria)]])

        return prediction
