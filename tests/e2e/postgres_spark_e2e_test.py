from bdd_helper import Given, When, Then
from postgres_spark import PostgresSpark


def test_read(postgres_spark: PostgresSpark):
    Given("query")
    query="SELECT * FROM clicks LIMIT 10"

    When("read table")
    df = postgres_spark.read(query)

    Then("is ok")
    assert df.count() == 10
    assert df.columns == ['id', 'campaign_id', 'created_at']

