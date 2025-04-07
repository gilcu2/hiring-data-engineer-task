from pyspark.sql import SparkSession, DataFrame


class PostgresSpark:
    def __init__(self, spark: SparkSession, url: str):
        self.url = url
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
