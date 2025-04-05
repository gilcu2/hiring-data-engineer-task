from pyspark.sql import SparkSession, DataFrame


class ClickHouse:
    def __init__(self, spark: SparkSession, host: str, port: str,
                 user: str, password: str, database: str):
        self.url = f"jdbc:ch://{host}:{port}/{database}?user={user}&password={password}"
        self.spark = spark

    def write(self, df:DataFrame, table:str):
        return (
            df
            .write
            .format("jdbc")
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("url", self.url)
            .option("dbtable", table)
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


