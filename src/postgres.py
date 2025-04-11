from urllib.parse import urlparse, parse_qs
import psycopg
from db import DB


class Postgres(DB):
    def __init__(self, url: str):
        print(f"Postgres url: {url}")
        parsed = urlparse(url.replace("jdbc:", "", 1))
        parsed_query = parse_qs(parsed.query)
        dbname=parsed.path.lstrip("/") if parsed.path else "postgres"
        self.client=psycopg.connect(
            f"host={parsed.hostname or 'localhost'} port={parsed.port or 8123} "
            f"dbname={dbname} user={parsed_query.get('user', ['postgres'])[0]} "
            f"password={parsed_query.get('password', ['postgres'])[0]}",
            autocommit=True,
        )

    def command(self, sql: str):
        cursor=self.client.cursor()
        cursor.execute(sql)

    def query(self, sql: str) -> list[any]:
        cursor = self.client.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    def create_table_as(self, new_table_name: str, source_table_name: str):
        self.command(f"CREATE TABLE {new_table_name} AS SELECT * FROM {source_table_name} WHERE FALSE")









