import os

os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"

from clickhouse import ClickHouse
from postgres import Postgres
from workflow import update_flow
from bdd_helper import Given, When, Then

RUN_LONG_TESTS = os.getenv("RUN_LONG_TESTS", "false").lower() == "true"


def test_workflow(clickhouse: ClickHouse, postgres: Postgres):
    Given("table names and date")
    ch_suffix = "_test"
    tables = ["advertiser", "campaign", "clicks", "impressions"]
    for table in tables:
        table_name = f"{table}{ch_suffix}"
        clickhouse.drop_table(table_name)

    test_date = postgres.get_extremes("impressions", "created_at").min_value.date()

    When("update")
    updated_rows = update_flow(test_date, test_date, ch_suffix=ch_suffix, limit=10)

    Then("is expected")
    updated_rows_arr = [updated_rows.advertiser, updated_rows.campaign, updated_rows.clicks, updated_rows.impressions]
    for i in range(4):
        r = clickhouse.query(f"SELECT COUNT(*) FROM {tables[i]}{ch_suffix}")
        assert r[0][0] > 0
        assert r[0][0] == updated_rows_arr[i]

    for table in tables:
        clickhouse.drop_table(f"{table}{ch_suffix}")
