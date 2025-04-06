from pipeline import update_entity, update_kpi
from postgres import Postgres
from clickhouse import ClickHouse
from bdd_helper import Given, When, Then, And
from datetime import datetime


def test_update_entity(postgres: Postgres, clickhouse: ClickHouse):
    Given("tables")
    table_name = "advertiser"
    test_table_name = "advertiser_test"

    When("update")
    clickhouse.create_table_as(test_table_name, table_name)
    n_rows = update_entity(table_name, test_table_name, postgres, clickhouse, limit=10)

    Then("is expected")
    assert n_rows > 0
    df = clickhouse.read(f"SELECT * FROM {test_table_name}")
    assert df.count() == n_rows
    clickhouse.drop_table(test_table_name)


def test_update_kpi_from_new_table(postgres: Postgres, clickhouse: ClickHouse):
    Given("tables")
    campaign_name = "campaign"
    clicks_table = "clicks"
    test_table_name = "clicks_test"
    test_date = datetime.strptime("2025-03-30", "%Y-%m-%d").date()

    When("update")
    clickhouse.create_table_as(test_table_name, clicks_table)
    n_rows = update_kpi(clicks_table, campaign_name, test_table_name,test_date,test_date,
                        postgres, clickhouse, limit=10)

    Then("is expected")
    df = clickhouse.read(f"SELECT * FROM {test_table_name}")
    assert df.count() == n_rows
    clickhouse.drop_table(test_table_name)
