class ReportIdentifier:
    def __init__(self, group, pi, volume):
        self.group = group
        self.pi = pi
        self.volume = volume

    def __hash__(self) -> int:
        return hash((self.group, self.pi, self.volume))

    def __eq__(self, o: "ReportIdentifier") -> bool:
        return self.group == o.group and self.pi == o.pi and self.volume == o.volume
