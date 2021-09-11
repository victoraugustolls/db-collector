import typing as t

from domain import entities


class Repository(t.Protocol):
    @classmethod
    async def create(cls, database: str):
        ...

    async def save_schema(self, schema: entities.Schema):
        ...

    async def get(self, query_id: str) -> t.Optional[entities.StatementStats]:
        ...

    async def list(self) -> list[entities.Statement]:
        ...

    async def set(self, timestamp: str, stmt: entities.Statement) -> None:
        ...

    async def set_plan(self, query_id: str, plan: entities.Plan) -> None:
        ...

    async def set_plan_by_id(self, query_id: str, plan: entities.Plan) -> None:
        ...

    async def debug(self) -> None:
        ...
