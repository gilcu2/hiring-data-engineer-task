from bdd_helper import Given, When, Then, And
from clickhouse import ClickHouse
from pyspark.sql import Row
from pyspark.sql import SparkSession
from pyspark.testing import assertDataFrameEqual
from pyspark.sql.types import StructType, StructField, LongType, StringType


def test_write_read(clickhouse: ClickHouse, spark: SparkSession):
    Given("dataframe and query")
    table_name = "test_table"
    data = [Row(id=11, name="John"), Row(id=12, name="Doe")]
    schema = StructType([
        StructField("id", LongType(), True),
        StructField("name", StringType(), True)
    ])
    df = spark.createDataFrame(data)
    query = "SELECT DISTINCT toInt64(id) as id, name FROM test_table"

    When("write table")
    clickhouse.write(df, table_name)

    And("read it")
    read_df = clickhouse.read(query, schema)

    Then("is expected")
    assertDataFrameEqual(read_df, df)

