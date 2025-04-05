from bdd_helper import Given, When, Then, And
from clickhouse import ClickHouse
from pyspark.sql import Row
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality


def test_write_read(clickhouse: ClickHouse, spark: SparkSession):
    Given("dataframe and query")
    table_name = "test_table"
    data = [Row(id=11, name="John"), Row(id=12, name="Doe")]
    df = spark.createDataFrame(data)
    query = "SELECT DISTINCT * FROM test_table"

    When("write table")
    clickhouse.write(df, table_name)

    And("read it")
    readed_df = clickhouse.read(query)

    Then("is expected")
    assert_df_equality(readed_df, df)

