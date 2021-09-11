import typing as t

from repository import Repository
from types import DSN


class Collector(t.Protocol):
    @classmethod
    async def create(cls, dsn: DSN, repository: Repository):
        ...

    async def start(self):
        ...

    async def take_snapshot(self):
        ...

    async def end(self):
        ...
