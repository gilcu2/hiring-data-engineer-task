from bdd_helper import Given, When, Then
from fastapi.testclient import TestClient
from kpi_api import app

client = TestClient(app)

def test_ctr_campaign():
    Given("path")
    path="/ctr_campaign"

    When("call")
    response = client.get(path, params={"limit": 10})

    Then("is expected")
    assert response.status_code == 200
    result=response.json()
    assert len(result)>0

def test_daily_impressions():
    Given("path")
    path="/daily_impressions"

    When("call")
    response = client.get(path, params={"limit": 10})

    Then("is expected")
    assert response.status_code == 200
    result=response.json()
    assert len(result)>0

def test_daily_clicks():
    Given("path")
    path="/daily_clicks"

    When("call")
    response = client.get(path, params={"limit": 10})

    Then("is expected")
    assert response.status_code == 200
    result=response.json()
    assert len(result)>0