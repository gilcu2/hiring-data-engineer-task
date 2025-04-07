from bdd_helper import Given, When, Then, And
from postgres import Postgres

def test_query(postgres:Postgres):
    Given("query")
    sql="SELECT created_at FROM clicks ORDER BY created_at DESC LIMIT 1"

    When("executed")
    r=postgres.query(sql)

    Then("is expected")
    assert r[0][0] is not None