from bdd_helper import Given, When, Then, And
from clickhouse import ClickHouse
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, LongType, StringType

create_sql = """
    CREATE TABLE {table_name} (
        id UInt32,
        name String,
    )
    ENGINE = MergeTree()
    ORDER BY id"""
query = "SELECT DISTINCT toInt64(id) as id, name FROM {table_name}"

def test_write_read(clickhouse: ClickHouse, spark: SparkSession):
    Given("dataframe")
    table_name = "test_table"
    data = [Row(id=11, name="John"), Row(id=12, name="Doe")]
    df = spark.createDataFrame(data)


    When("create table")
    clickhouse.command(create_sql.format(table_name=table_name))
    clickhouse.write(df, table_name)

    And("read it")
    read_df = clickhouse.read(query.format(table_name=table_name))

    Then("is expected")
    assertDataFrameEqual(read_df, df)
    clickhouse.drop_table(table_name)


def test_create_table_as(clickhouse: ClickHouse):
    Given("tables")
    new_table_name = "test_new"
    source_table_name = "advertiser"

    When("create")
    clickhouse.create_table_as(new_table_name, source_table_name)

    Then("is ok")
    clickhouse.drop_table(new_table_name)


def test_swap_tables(clickhouse: ClickHouse):
    Given("tables")
    table_name = "test"
    new_table_name = "test_new"
    old_table_name = "test_old"
    source_table_name = "advertiser"

    When("create and swap")
    clickhouse.create_table_as(table_name, source_table_name)
    clickhouse.create_table_as(new_table_name, source_table_name)
    clickhouse.swap_tables(table_name, new_table_name, old_table_name)

    Then("is ok")
    clickhouse.drop_table(old_table_name)
    clickhouse.drop_table(table_name)

def test_replace_table_data(clickhouse: ClickHouse, spark: SparkSession):
    Given("table and new data")
    table_name = "test"

    data = [Row(id=11, name="John"), Row(id=12, name="Doe")]
    df = spark.createDataFrame(data)
    clickhouse.command(create_sql.format(table_name=table_name))
    clickhouse.write(df, table_name)

    And("new data")
    new_data = [Row(id=12, name="Johana"), Row(id=13, name="Dane")]
    new_df = spark.createDataFrame(new_data)

    When("replace table data")
    clickhouse.replace_table_data(table_name,new_df)

    And("read data")
    read_df = clickhouse.read(query.format(table_name=table_name))

    Then("is expected")
    assertDataFrameEqual(read_df, new_df)
    clickhouse.drop_table(table_name)