import ast
from datetime import datetime

import aiomysql

from collector import Collector
from domain import entities
from repository import Repository
from types import DSN
from . import _queries


class MySQLCollector(Collector):
    _pool: aiomysql.Pool
    _repository: Repository
    _database: str
    _tables: str
    _observe: bool
    _observed_queries: set

    def __init__(self, pool: aiomysql.Pool, repository: Repository, database: str):
        self._pool = pool
        self._repository = repository
        self._database = database
        self._tables = ""
        self._observe = True
        self._observed_queries = set()

    @classmethod
    async def create(cls, dsn: DSN, repository: Repository) -> "MySQLCollector":
        pool = await aiomysql.create_pool(
            host=dsn.host, user=dsn.user, password=dsn.password, port=dsn.port, db=dsn.database,
            autocommit=True,
        )
        return cls(pool=pool, repository=repository, database=dsn.database)

    async def start(self) -> None:
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_queries.schema_query)
                rows = await cur.fetchall()

        tables: list[entities.Table] = []
        for row in rows:
            columns: list[entities.Column] = []
            for column in ast.literal_eval(row[1]):
                columns.append(
                    entities.Column(
                        name=column["name"],
                        type=column["type"],
                        nullable=column["nullable"] == "YES",
                    )
                )

            tables.append(entities.Table(name=row[0], columns=columns))

        self._tables = "|".join(map(lambda x: x.name.split(".")[1], tables))
        await self._repository.save_schema(schema=entities.Schema(tables=tables))

    async def take_snapshot(self) -> None:
        now: datetime = datetime.utcnow()
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(_queries.statements_query, (self._tables, '%EXPLAIN%'))
                rows = await cur.fetchall()

        for row in rows:
            stmt = entities.Statement(
                query_id=row[0],
                query=row[2],
                normalized_query=row[2],
                calls=row[1],
                total_exec_time=float(row[3]),
                mean_exec_time=float(row[4]),
            )
            await self._repository.set(timestamp=str(now), stmt=stmt)

    async def end(self) -> None:
        stmts = await self._repository.list()

        async with self._pool.acquire() as conn:
            for stmt in stmts:
                try:
                    async with conn.cursor() as cur:
                        await cur.execute(_queries.activity_query, (int(stmt.query_id),))
                        (query,) = await cur.fetchone()
                        await cur.execute(f"explain format = json {query};")
                        (result,) = await cur.fetchone()
                except:
                    continue
                else:
                    await self._repository.set_plan_by_id(stmt.query_id, entities.Plan(result))
