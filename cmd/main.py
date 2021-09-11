import asyncio
from datetime import datetime

import uvloop

from collector import Collector, MySQLCollector, PostgreSQLCollector
from config import settings
from repository import InMemoryRepo, Repository, SQLiteRepo
from types import DSN


async def main():
    repository = await new_repository(settings.repository.type)
    collector = await new_collector(config=settings.collector, repository=repository)

    count: int = 0

    await collector.start()
    while count <= settings.loop.cycles:
        await collector.take_snapshot()
        await asyncio.sleep(settings.loop.sleep)
        count += 1

    await collector.end()
    await repository.debug()

    return


async def new_repository(value: str) -> Repository:
    if value == "sqlite":
        return await SQLiteRepo.create(database=f"../collector/collector_{str(datetime.utcnow())}.db")
    elif value == "in_memory":
        return await InMemoryRepo.create()
    else:
        raise Exception("invalid repository type")


async def new_collector(config, repository: Repository) -> Collector:
    dsn = DSN(
        user=config.user,
        password=config.password,
        host=config.host,
        port=config.port,
        database=config.database,
    )

    if config.rdbms == "postgres":
        return await PostgreSQLCollector.create(repository=repository, dsn=dsn)
    elif config.rdbms == "mysql":
        return await MySQLCollector.create(repository=repository, dsn=dsn)
    else:
        raise Exception("invalid collector rdbms")


if __name__ == "__main__":
    uvloop.install()
    asyncio.run(main())
