from pyspark.sql import SparkSession


def create_spark(app_name:str) -> SparkSession:
    return SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.driver.extraClassPath", "./driver/postgresql-42.7.5.jar") \
        .getOrCreate()
