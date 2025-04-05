from bdd_helper import Given, When, Then, And
from spark import create_spark

def create_spark_test():
    Given("app name")
    app_name="test_spark"

    When("create spark session")
    spark = create_spark(app_name)

    Then("is ok")
    assert spark.sparkContext.appName == app_name