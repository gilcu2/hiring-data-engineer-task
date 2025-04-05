from pyspark.sql import SparkSession, DataFrame


class Postgres:
    def __init__(self, spark: SparkSession, host: str, port: str,
                 user: str, password: str, database: str):
        self.url = f"jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}"
        self.spark = spark


    def read(self, query: str) -> DataFrame:
        return (
            self.spark
            .read
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", self.url)
            .option("query", query)
            .load()
        )
