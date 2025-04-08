import pytest
from pyspark.sql import SparkSession
from typing import Generator
from spark import create_spark
from clickhouse import ClickHouse
from postgres import Postgres
from postgres_spark import PostgresSpark
from clickhouse_spark import ClickHouseSpark
from prefect.testing.utilities import prefect_test_harness

@pytest.fixture()
def clickhouse()   -> Generator[ClickHouse, None, None]:
    host = "localhost"
    port = "8123"
    user = "default"
    password = "12345"
    database = "default"
    url = f"jdbc:ch://{host}:{port}/{database}?user={user}&password={password}"
    yield ClickHouse(url)

@pytest.fixture()
def postgres()   -> Generator[ClickHouse, None, None]:
    host = "localhost"
    port = "5432"
    user = "postgres"
    password = "postgres"
    database = "postgres"
    url = f"jdbc:ch://{host}:{port}/{database}?user={user}&password={password}"
    yield Postgres(url)

@pytest.fixture()
def spark() -> Generator[SparkSession, None, None]:
    yield create_spark("test_spark_session")

@pytest.fixture()
def postgres_spark(spark:SparkSession)   -> Generator[PostgresSpark, None, None]:
    host = "localhost"
    port = "5432"
    user = "postgres"
    password = "postgres"
    database = "postgres"
    url = f"jdbc:postgresql://{host}:{port}/{database}?user={user}&password={password}"
    yield PostgresSpark(spark, url)

@pytest.fixture()
def clickhouse_spark(spark:SparkSession, clickhouse:ClickHouse)   -> Generator[ClickHouseSpark, None, None]:
    host = "localhost"
    port = "8123"
    user = "default"
    password = "12345"
    database = "default"
    url = f"jdbc:ch://{host}:{port}/{database}?user={user}&password={password}"
    yield ClickHouseSpark(spark, url,clickhouse)

@pytest.fixture(autouse=True, scope="session")
def prefect_test_fixture():
    with prefect_test_harness():
        yield