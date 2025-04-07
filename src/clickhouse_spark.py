from pyspark.sql import SparkSession, DataFrame
from urllib.parse import urlparse, parse_qs
import clickhouse_connect

from clickhouse import ClickHouse


class ClickHouseSpark:
    def __init__(self, spark: SparkSession, url: str, clickhouse: ClickHouse):
        self.clickhouse = clickhouse
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

    def replace_table_data(self, table_name: str, df: DataFrame):
        new_table_name = f"{table_name}_new"
        old_table_name = f"{table_name}_old"

        self.clickhouse.create_table_as(new_table_name, table_name)
        self.write(df, new_table_name)

        self.clickhouse.swap_tables(table_name, new_table_name, old_table_name)
        self.clickhouse.drop_table(old_table_name)
