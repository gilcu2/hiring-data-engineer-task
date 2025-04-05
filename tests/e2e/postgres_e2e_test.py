from bdd_helper import Given, When, Then, And
from postgres import Postgres


def test_read(postgres: Postgres):
    Given("query")
    query="SELECT * FROM clicks LIMIT 10"

    When("read table")
    df = postgres.read(query)

    Then("is ok")
    assert df.count() == 10
    assert df.columns == ['id', 'campaign_id', 'created_at']

