from pyspark.sql import SparkSession
import os

SPARK_MASTER_URL=os.getenv("SPARK_MASTER_URL", "local[2]")


def create_spark(app_name:str) -> SparkSession:
    return SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.jars", "./drivers/postgresql-42.7.5.jar,./drivers/clickhouse-jdbc-0.6.3-all.jar") \
        .master(SPARK_MASTER_URL) \
        .getOrCreate()


