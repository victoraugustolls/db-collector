import dataclasses


@dataclasses.dataclass
class StatementStats:
    calls: int
    total_exec_time: float
    mean_exec_time: float
