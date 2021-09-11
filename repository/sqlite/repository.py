import ast
import json
import typing as t

import aiosqlite

from domain import entities
from . import _schema, _queries
from ..repository import Repository


class SQLiteRepo(Repository):
    _conn: aiosqlite.Connection
    _queries_cache: dict[str, bool]

    def __init__(self):
        self._queries_cache = {}

    @classmethod
    async def create(cls, database: str) -> "SQLiteRepo":
        self = SQLiteRepo()
        conn = await aiosqlite.connect(database=database)
        _ = await conn.execute(_schema.schema)
        _ = await conn.execute(_schema.statement)
        _ = await conn.execute(_schema.statement_execution)
        await conn.commit()
        self._conn = conn
        return self

    async def save_schema(self, schema: entities.Schema):
        parameters = [table.as_tuple() for table in schema.tables]
        await self._conn.executemany(_queries.insert_schema, parameters)
        await self._conn.commit()

    async def get(self, query_id: str) -> t.Optional[entities.StatementStats]:
        cursor: aiosqlite.Cursor = await self._conn.execute(_queries.statement_stats, [query_id])
        row: aiosqlite.Row = await cursor.fetchone()
        if row is None:
            return None

        return entities.StatementStats(
            calls=row["calls"],
            total_exec_time=row["total_execution_time"],
            mean_exec_time=row["mean_execution_time"],
        )

    async def list(self) -> list[entities.Statement]:
        rows = await self._conn.execute_fetchall(_queries.list_statements)
        return [entities.Statement(row[0], row[1], "", 0, 0, 0) for row in rows]

    async def set(self, timestamp: str, stmt: entities.Statement) -> None:
        if self._queries_cache.get(stmt.query_id) is None:
            self._queries_cache[stmt.query_id] = True
            await self._conn.execute(_queries.insert_statement, [stmt.query_id, stmt.query, stmt.normalized_query])

        _ = await self._conn.execute(
            _queries.insert_statement_stats,
            [
                stmt.query_id,
                stmt.calls,
                stmt.total_exec_time,
                stmt.mean_exec_time,
                timestamp,
            ]
        )
        await self._conn.commit()

    async def set_plan(self, query_id: str, plan: entities.Plan) -> None:
        await self._conn.execute(_queries.set_plan, [plan.raw, query_id])

    async def set_plan_by_id(self, query_id: str, plan: entities.Plan) -> None:
        await self._conn.execute(_queries.set_plan_by_id, [plan.raw, query_id])

    async def debug(self) -> None:
        rows = await self._conn.execute_fetchall(_queries.debug_schema)
        print("SCHEMA:")
        for row in rows:
            print(f"\tTable: {row[0]}")
            print("\tColumns:")
            print("\t\tName | Type | Nullable")
            for column in json.loads(row[1]):
                print(f"\t\t{column['name']} | {column['type']} | {column['nullable']}")
            print()
        print()

        rows = await self._conn.execute_fetchall(_queries.debug_statements)
        print("QUERIES:")
        for row in rows:
            print(f"Query ID: {row[0]}")
            print(f"\t Query: {row[1]}")
            print(f"\t Plan: {row[2]}")
            stats = ast.literal_eval(row[3])
            calls = 0
            for stat in stats:
                # Only print snapshot if there where changes
                if stat["calls"] <= calls:
                    continue
                calls = stat["calls"]
                print(f"\t Snapshot: {stat['created_at']}")
                print(
                    f"\t\t Calls: {calls}\n"
                    f"\t\t Mean execution time: {stat['total_execution_time']}\n"
                    f"\t\t Total execution time: {stat['mean_execution_time']}"
                )
            print()
