from datetime import datetime

import asyncpg

from collector import Collector
from domain import entities
from repository.in_memory import InMemoryRepo

_statements_query = """
select
    queryid,
    query,
    calls,
    total_exec_time,
    mean_exec_time
from pg_stat_statements;
"""


class PostgreSQLCollector(Collector):
    _conn: asyncpg.Pool
    _repo: InMemoryRepo

    def __init__(self, conn: asyncpg.Pool, repo: InMemoryRepo):
        self._conn = conn
        self._repo = repo

    async def query_statements(self):
        now: datetime = datetime.utcnow()
        rows: list[asyncpg.Record] = await self._conn.fetch(_statements_query)
        for row in rows:
            stmt = entities.Statement(
                query_id=row["queryid"],
                query=row["query"],
                calls=row["calls"],
                total_exec_time=row["total_exec_time"],
                mean_exec_time=row["mean_exec_time"],
            )
            self._repo.set(snapshot=str(now), stmt=stmt)
