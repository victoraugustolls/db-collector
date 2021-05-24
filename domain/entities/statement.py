import dataclasses

from domain.entities.statement_stats import StatementStats


@dataclasses.dataclass
class Statement:
    query_id: int
    query: str
    calls: int
    total_exec_time: float
    mean_exec_time: float

    def stats(self) -> StatementStats:
        return StatementStats(
            calls=self.calls,
            total_exec_time=self.total_exec_time,
            mean_exec_time=self.mean_exec_time,
        )
