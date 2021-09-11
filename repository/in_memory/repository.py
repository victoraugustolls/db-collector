import typing as t

from domain import entities
from ..repository import Repository


class InMemoryRepo(Repository):
    _schema: entities.Schema
    _db: dict[str, dict[str, entities.StatementStats]]
    _queries: dict[str, str]

    def __init__(self):
        self._db = {}
        self._queries = {}

    @classmethod
    async def create(cls, _: str = ""):
        return InMemoryRepo()

    async def save_schema(self, schema: entities.Schema):
        self._schema = schema

    async def get(self, query_id: str) -> t.Optional[entities.StatementStats]:
        return self._db[query_id].popitem()[1]

    async def set(self, timestamp: str, stmt: entities.Statement) -> None:
        if self._db.get(stmt.query_id) is None:
            self._db[stmt.query_id] = {}
        if self._queries.get(stmt.query_id) is None:
            self._queries[stmt.query_id] = stmt.query
        self._db[stmt.query_id][timestamp] = stmt.stats()

    async def debug(self) -> None:
        print("SCHEMA:")
        for table in self._schema.tables:
            print(f"\tTable: {table.name}")
            print("\tColumns:")
            print("\t\tName | Type | Nullable")
            for column in table.columns:
                print(f"\t\t{column.name} | {column.type} | {column.nullable}")
            print()
        print()

        print("QUERIES:")
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
