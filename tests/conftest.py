import pytest
from pyspark.sql import SparkSession
from typing import Generator
from spark import create_spark
from postgres import Postgres
from clickhouse import ClickHouse

@pytest.fixture()
def spark() -> Generator[SparkSession, None, None]:
    yield create_spark("test_spark_session")

@pytest.fixture()
def postgres(spark:SparkSession)   -> Generator[Postgres, None, None]:
    yield Postgres(spark,"localhost","5432","postgres","postgres","postgres")

@pytest.fixture()
def clickhouse(spark:SparkSession)   -> Generator[ClickHouse, None, None]:
    yield ClickHouse(spark,"localhost","8123","default","12345","default")