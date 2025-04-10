from bdd_helper import Given, When, Then, And
from clickhouse_spark import ClickHouseSpark
from clickhouse import ClickHouse
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual


create_sql = """
    CREATE TABLE {table_name} (
        id UInt32,
        name String,
    )
    ENGINE = MergeTree()
    ORDER BY id"""
query = "SELECT DISTINCT toInt64(id) as id, name FROM {table_name}"


def test_write_read(clickhouse_spark: ClickHouseSpark, spark: SparkSession, clickhouse: ClickHouse):
    Given("dataframe")
    table_name = "test_table"
    data = [Row(id=11, name="John"), Row(id=12, name="Doe")]
    df = spark.createDataFrame(data)

    When("create table")
    clickhouse.command(create_sql.format(table_name=table_name))
    clickhouse_spark.write(df, table_name)

    And("read it")
    read_df = clickhouse_spark.read(query.format(table_name=table_name))

    Then("is expected")
    assertDataFrameEqual(read_df, df)
    clickhouse.drop_table(table_name)


def test_replace_table_data(clickhouse_spark: ClickHouseSpark, spark: SparkSession, clickhouse: ClickHouse):
    Given("table and new data")
    table_name = "test"

    data = [Row(id=11, name="John"), Row(id=12, name="Doe")]
    df = spark.createDataFrame(data)
    clickhouse.command(create_sql.format(table_name=table_name))
    clickhouse_spark.write(df, table_name)

    And("new data")
    new_data = [Row(id=12, name="Johana"), Row(id=13, name="Dane")]
    new_df = spark.createDataFrame(new_data)

    When("replace table data")
    clickhouse_spark.replace_table_data(table_name, new_df)

    And("read data")
    read_df = clickhouse_spark.read(query.format(table_name=table_name))

    Then("is expected")
    assertDataFrameEqual(read_df, new_df)
    clickhouse.drop_table(table_name)
