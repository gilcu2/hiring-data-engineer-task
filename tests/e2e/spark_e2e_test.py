from pyspark.sql import SparkSession

from bdd_helper import Given, When, Then, And


def test_create_spark(spark:SparkSession):
    Given("the spark session fixture")

    Then("is expected")
    assert spark.sparkContext.appName == "test_spark_session"