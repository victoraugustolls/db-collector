import typing as t


class Collector(t.Protocol):
    def query_statements(self):
        ...
