from typing import Protocol
from typing import Optional
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")

@dataclass
class Extremes(Generic[T]):
    min_value: T
    max_value: T

class DB(Protocol):

    def command(self, sql: str):
        ...

    def query(self, sql: str) -> list:
        ...

    def drop_table(self, table_name: str):
        self.command(f"DROP TABLE IF EXISTS {table_name} ")

    def get_extremes(self, table_name: str, column_name: str) -> Optional[Extremes]:
        sql = f"SELECT Count(*),MIN({column_name}),MAX({column_name}) FROM {table_name}"
        [count, min_value, max_value] = self.query(sql)[0]
        if count > 0:
            return Extremes(min_value, max_value)
        else:
            return None

    def create_table_as(self, new_table_name: str, source_table_name: str):
        ...