import os
from datetime import datetime
import time
import asyncio

os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"

from clickhouse import ClickHouse
from workflow import update_flow, main
from bdd_helper import Given, When, Then
from prefect.cli.deployment import delete as delete_deployment
import pytest

RUN_LONG_TESTS = os.getenv("RUN_LONG_TESTS", "false").lower() == "true"


def test_workflow(clickhouse: ClickHouse):
    Given("table names and date")
    ch_suffix = "_test"
    tables = ["advertiser", "campaign", "clicks", "impressions"]
    for table in tables:
        table_name = f"{table}{ch_suffix}"
        clickhouse.drop_table(table_name)

    test_date = datetime.strptime("2025-03-30", "%Y-%m-%d")

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


