import asyncio

import asyncpg
import uvloop

from collector.postgresql.collector import Collector
from repository.in_memory import InMemoryRepo


async def main():
    pool: asyncpg.Pool = await asyncpg.create_pool(
        dsn="postgresql://postgres:postgres@127.0.0.1:5432/dev",
        min_size=2,
        max_size=5,
    )
    repo = InMemoryRepo()
    collector = Collector(conn=pool, repo=repo)
    count: int = 0
    while count <= 3:
        await collector.query_statements()
        await asyncio.sleep(5)
        count += 1

    repo.debug()


uvloop.install()
asyncio.run(main())
