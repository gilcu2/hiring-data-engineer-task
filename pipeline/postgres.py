from pyspark.sql import SparkSession, DataFrame


class Postgres:
    def __init__(self, spark: SparkSession, host: str, port: str,
                 user: str, password: str, database: str):
        self.url = f"jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}"

    def read_table(self, query: str) -> DataFrame:
        return (
            self.spark
            .read
            .format("jdbc")
            .option("url", self.url)
            .option("query", query)
        )
