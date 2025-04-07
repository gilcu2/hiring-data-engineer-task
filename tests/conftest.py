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
    host = "localhost"
    port = "5432"
    user = "postgres"
    password = "postgres"
    database = "postgres"
    url = f"jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}"
    yield Postgres(spark,url)

@pytest.fixture()
def clickhouse(spark:SparkSession)   -> Generator[ClickHouse, None, None]:
    host = "localhost"
    port = "8123"
    user = "default"
    password = "12345"
    database = "default"
    url = f"jdbc:ch://{host}:{port}/{database}?user={user}&password={password}"
    yield ClickHouse(spark,url)