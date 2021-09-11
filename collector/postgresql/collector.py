import ast
import asyncio
import re
from datetime import datetime

import asyncpg
from sql_metadata import Parser

from collector import Collector
from domain import entities
from repository import Repository
from types import DSN
from . import _queries


class PostgreSQLCollector(Collector):
    _pool: asyncpg.Pool
    _repository: Repository
    _database: str
    _tables: list[str]
    _observe: bool
    _observed_queries: set

    def __init__(self, pool: asyncpg.Pool, repository: Repository, database: str):
        self._pool = pool
        self._repository = repository
        self._database = database
        self._tables = []
        self._observe = True
        self._observed_queries = set()

    @classmethod
    async def create(cls, dsn: DSN, repository: Repository) -> "PostgreSQLCollector":
        pool = await asyncpg.create_pool(
            dsn=f"postgresql://{dsn.user}:{dsn.password}@{dsn.host}:{dsn.port}/{dsn.database}",
        )
        return cls(pool=pool, repository=repository, database=dsn.database)

    async def observe_activity(self) -> None:
        q = _queries.activity_query
        async with self._pool.acquire() as conn:
            while self._observe is True:
                queries = await conn.fetch(q)
                for query in queries:
                    self._observed_queries.add(query["query"])

    async def start(self) -> None:
        # Start background "pg_stat_activity" observer
        asyncio.get_running_loop().create_task(self.observe_activity())

        rows: list[asyncpg.Record] = await self._pool.fetch(_queries.schema_query, self._database)
        tables: list[entities.Table] = []

        for row in rows:
            columns: list[entities.Column] = []
            for column in ast.literal_eval(row["columns"]):
                columns.append(
                    entities.Column(
                        name=column["name"],
                        type=column["type"],
                        nullable=column["nullable"] == "YES",
                    )
                )

            tables.append(entities.Table(name=row["table_name"], columns=columns))
            self._tables.append(row["table_name"].split(".")[1])

        await self._repository.save_schema(schema=entities.Schema(tables=tables))

    async def take_snapshot(self) -> None:
        now: datetime = datetime.utcnow()
        rows: list[asyncpg.Record] = await self._pool.fetch(_queries.statements_query, self._database)
        for row in rows:
            query: str = row["query"]
            if any(table in query for table in self._tables) is False:
                continue

            if query.startswith("explain"):
                continue

            stmt = entities.Statement(
                query_id=row["queryid"],
                query=row["query"],
                normalized_query=self.generalize_query(row["query"]),
                calls=row["calls"],
                total_exec_time=row["total_exec_time"],
                mean_exec_time=row["mean_exec_time"],
            )
            await self._repository.set(timestamp=str(now), stmt=stmt)

    async def end(self):
        # Ends async task
        self._observe = False

        # Waits for async task to end
        await asyncio.sleep(0.5)

        # For each captured query in "pg_stat_activity", try to acquire the execution plan and update the repository
        for query in self._observed_queries:
            try:
                result = await self._pool.fetchval(f"explain (format json) {query};")
            except:
                continue
            else:
                await self._repository.set_plan(self.generalize_query(query), entities.Plan(result))

    @staticmethod
    def generalize_query(query: str) -> str:
        return re.sub(r"\s?=\s?[XN]", "", Parser(query).generalize.replace("$", ""))
