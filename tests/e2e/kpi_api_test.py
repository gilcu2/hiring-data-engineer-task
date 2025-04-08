from bdd_helper import Given, When, Then, And
from fastapi.testclient import TestClient
from kpi_api import app

client = TestClient(app)

def test_ctr_campaign():
    Given("path")
    path="/ctr_campaign"

    When("call")
    response = client.get(path)

    Then("is expected")
    assert response.status_code == 200
    result=response.json()
    assert len(result)>0