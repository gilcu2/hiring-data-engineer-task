from urllib.parse import urlparse, parse_qs

import clickhouse_connect
from clickhouse_connect.driver.types import Matrix
from db import DB


class ClickHouse(DB):
    def __init__(self, url: str):
        print(f"Clickhouse url: {url}")
        parsed = urlparse(url.replace("jdbc:", "", 1))
        parsed_query = parse_qs(parsed.query)
        dbname = parsed.path.lstrip("/") if parsed.path else "default"
        self.client = clickhouse_connect.get_client(
            host=parsed.hostname or 'localhost',
            port=parsed.port or 8123,
            username=parsed_query.get('user', ['default'])[0],
            password=parsed_query.get('password', ['12345'])[0],
            database=dbname
        )

    def command(self, sql: str):
        self.client.command(sql)

    def query(self, sql: str) -> Matrix:
        r = self.client.query(sql)
        return r.result_set

    def create_table_as(self, new_table_name: str, source_table_name: str):
        self.client.command(f"CREATE TABLE {new_table_name} AS {source_table_name}")

    def swap_tables(self, table_name: str, new_table_name: str, old_table_name: str):
        self.client.command(f"RENAME TABLE {table_name} TO {old_table_name}, {new_table_name} TO {table_name}")
