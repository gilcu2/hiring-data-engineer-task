from black import Sequence
from pyspark.sql import SparkSession, DataFrame
from urllib.parse import urlparse, parse_qs
import clickhouse_connect


class ClickHouse:
    def __init__(self, spark: SparkSession, url: str):
        self.url = f"{url}&jdbcCompliant=false"
        self.spark = spark
        parsed = urlparse(url.replace("jdbc:", "", 1))
        parsed_query=parse_qs(parsed.query)
        self.client = clickhouse_connect.get_client(
            host=parsed.hostname or 'localhost',
            port=parsed.port or 8123,
            username=parsed_query.get('user',['default'])[0],
            password=parsed_query.get('password',['12345'])[0]
        )

    def write(self, df: DataFrame, table_name: str):
        return (
            df
            .write
            .format("jdbc")
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("autocommit", "true")
            .option("batchsize", 10000)
            .option("url", self.url)
            .option("dbtable", table_name)
            .mode("append")
            .save()
        )

    def read(self, query: str) -> DataFrame:
        return (
            self.spark
            .read
            .format("jdbc")
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("url", self.url)
            .option("query", query)
            .load()
        )

    def command(self, sql: str):
        self.client.command(sql)

    def query(self, sql: str) -> Sequence[Sequence]:
        r = self.client.query(sql)
        return r.result_set

    def create_table_as(self, new_table_name: str, source_table_name: str):
        self.client.command(f"CREATE TABLE {new_table_name} AS {source_table_name}")

    def drop_table(self, table_name: str):
        self.client.command(f"DROP TABLE IF EXISTS {table_name} ")

    def swap_tables(self, table_name: str, new_table_name: str, old_table_name: str):
        self.client.command(f"RENAME TABLE {table_name} TO {old_table_name}, {new_table_name} TO {table_name}")

    def replace_table_data(self, table_name: str, df: DataFrame):
        new_table_name = f"{table_name}_new"
        old_table_name = f"{table_name}_old"

        self.create_table_as(new_table_name, table_name)
        self.write(df, new_table_name)

        self.swap_tables(table_name, new_table_name, old_table_name)
        self.drop_table(old_table_name)
