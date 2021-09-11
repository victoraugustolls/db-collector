import dataclasses
import json


@dataclasses.dataclass
class Column:
    name: str
    type: str
    nullable: bool


@dataclasses.dataclass
class Table:
    name: str
    columns: list[Column]

    def as_tuple(self) -> tuple[str, str]:
        str_columns = json.dumps([column.__dict__ for column in self.columns])
        return self.name, str_columns


@dataclasses.dataclass
class Schema:
    tables: list[Table]
