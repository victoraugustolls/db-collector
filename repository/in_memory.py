import typing as t

from domain import entities


class InMemoryRepo:
    _db: dict[int, dict[str, entities.StatementStats]]
    _queries: dict[int, str]

    def __init__(self):
        self._db = {}
        self._queries = {}

    def get(self, snapshot: str, query_id: int) -> t.Optional[entities.StatementStats]:
        return self._db[query_id][snapshot]

    def set(self, snapshot: str, stmt: entities.Statement) -> None:
        if self._db.get(stmt.query_id) is None:
            self._db[stmt.query_id] = {}
        if self._queries.get(stmt.query_id) is None:
            self._queries[stmt.query_id] = stmt.query
        self._db[stmt.query_id][snapshot] = stmt.stats()

    def debug(self) -> None:
        for k, v in self._db.items():
            print(f"Query ID: {k}")
            print(f"\t Query: {self._queries[k]}")
            for snapshot, stmt in v.items():
                print(f"\t Snapshot: {snapshot}")
                print(
                    f"\t\t Calls: {stmt.calls}\n"
                    f"\t\t Mean execution time: {stmt.mean_exec_time}\n"
                    f"\t\t Total execution time: {stmt.total_exec_time}"
                )
            print()
