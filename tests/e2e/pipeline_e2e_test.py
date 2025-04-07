from pipeline import update_entity, update_kpi, update_clickhouse, main, get_update_interval
from clickhouse import ClickHouse
from postgres import Postgres
from postgres_spark import PostgresSpark
from clickhouse_spark import ClickHouseSpark
from bdd_helper import Given, When, Then, And
from datetime import datetime


def test_update_entity(postgres_spark: PostgresSpark, clickhouse_spark: ClickHouseSpark, clickhouse: ClickHouse):
    Given("tables")
    table_name = "advertiser"
    test_table_name = "advertiser_test"

    When("update")
    clickhouse.create_table_as(test_table_name, table_name)
    n_rows = update_entity(table_name, test_table_name, postgres_spark, clickhouse_spark, limit=10)

    Then("is expected")
    assert n_rows > 0
    df = clickhouse_spark.read(f"SELECT * FROM {test_table_name}")
    assert df.count() == n_rows
    clickhouse.drop_table(test_table_name)


def test_update_kpi_from_new_table(postgres_spark: PostgresSpark, clickhouse_spark: ClickHouseSpark,
                                   clickhouse: ClickHouse):
    Given("tables")
    campaign_table = "campaign"
    clicks_table = "clicks"
    test_table = "clicks_test"
    test_date = datetime.strptime("2025-03-30", "%Y-%m-%d").date()

    When("update")
    clickhouse.drop_table(test_table)
    clickhouse.create_table_as(test_table, clicks_table)
    n_rows = update_kpi(clicks_table, campaign_table, test_table, test_date, test_date,
                        postgres_spark, clickhouse_spark, limit=10)

    Then("is expected")
    r = clickhouse.query(f"SELECT COUNT(*) FROM {test_table}")
    assert r[0][0] == n_rows
    r = clickhouse.query(f"SELECT COUNT(*) FROM {test_table} WHERE toDate(created_at) = '{test_date}'")
    assert r[0][0] == n_rows
    clickhouse.drop_table(test_table)


def test_update_kpi_from_previous_table(postgres_spark: PostgresSpark, clickhouse_spark: ClickHouseSpark,
                                        clickhouse: ClickHouse):
    Given("tables")
    campaign_table = "campaign"
    clicks_table = "clicks"
    test_table = "clicks_test"
    test_date0 = datetime.strptime("2025-03-30", "%Y-%m-%d").date()
    test_date1 = datetime.strptime("2025-03-31", "%Y-%m-%d").date()

    When("update")
    clickhouse.drop_table(test_table)
    clickhouse.create_table_as(test_table, clicks_table)
    n_rows0 = update_kpi(clicks_table, campaign_table, test_table, test_date0, test_date0,
                         postgres_spark, clickhouse_spark, limit=10)
    And("update next day")
    n_rows1 = update_kpi(clicks_table, campaign_table, test_table, test_date1, test_date1,
                         postgres_spark, clickhouse_spark, limit=10)

    Then("is expected")
    r = clickhouse.query(f"SELECT COUNT(*) FROM {test_table}")
    assert r[0][0] == n_rows0 + n_rows1
    r = clickhouse.query(f"SELECT COUNT(*) FROM {test_table} WHERE toDate(created_at) = '{test_date0}'")
    assert r[0][0] == n_rows0
    r = clickhouse.query(f"SELECT COUNT(*) FROM {test_table} WHERE toDate(created_at) = '{test_date1}'")
    assert r[0][0] == n_rows1
    clickhouse.drop_table(test_table)


def test_update_clickhouse(postgres_spark: PostgresSpark, clickhouse_spark: ClickHouseSpark, clickhouse: ClickHouse):
    Given("tables and date")
    ch_suffix = "_test"
    tables = ["advertiser", "campaign", "clicks", "impressions"]
    test_date = datetime.strptime("2025-03-30", "%Y-%m-%d").date()
    for table in tables:
        clickhouse.create_table_as(f"{table}{ch_suffix}", table)

    When("update")
    updated_rows = update_clickhouse(test_date, test_date, postgres_spark, clickhouse_spark, ch_suffix, limit=10)

    Then("is expected")
    for i in range(4):
        r = clickhouse.query(f"SELECT COUNT(*) FROM {tables[i]}{ch_suffix}")
        assert r[0][0] > 0
        assert r[0][0] == updated_rows[i]

    for table in tables:
        clickhouse.drop_table(f"{table}{ch_suffix}")


def test_main(clickhouse_spark: ClickHouseSpark, clickhouse: ClickHouse):
    Given("tables and date")
    ch_suffix = "_test"
    tables = ["advertiser", "campaign", "clicks", "impressions"]
    test_date = datetime.strptime("2025-03-30", "%Y-%m-%d")

    When("update")
    updated_rows = main(test_date, test_date, ch_suffix=ch_suffix, limit=10)

    Then("is expected")
    for i in range(4):
        r = clickhouse.query(f"SELECT COUNT(*) FROM {tables[i]}{ch_suffix}")
        assert r[0][0] > 0
        assert r[0][0] == updated_rows[i]

    for table in tables:
        clickhouse.drop_table(f"{table}{ch_suffix}")

def test_get_update_interval_empty_tables(postgres:Postgres,clickhouse:ClickHouse):
    Given("empty tables")
    table_name='impressions_test'
    postgres.drop_table(table_name)
    clickhouse.drop_table(table_name)
    postgres.create_table_as(table_name,'impressions')
    clickhouse.create_table_as(table_name,'impressions')

    When("compute interval")
    interval=get_update_interval(postgres,clickhouse,table_name,table_name)

    Then("is expected")
    assert interval is None



