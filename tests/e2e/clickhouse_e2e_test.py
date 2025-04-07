from bdd_helper import Given, When, Then, And
from clickhouse import ClickHouse

create_sql = """
    CREATE TABLE {table_name} (
        id UInt32,
        name String,
    )
    ENGINE = MergeTree()
    ORDER BY id"""
query = "SELECT DISTINCT toInt64(id) as id, name FROM {table_name}"


def test_create_table_as(clickhouse: ClickHouse):
    Given("tables")
    new_table_name = "test_new"
    source_table_name = "advertiser"

    When("create")
    clickhouse.create_table_as(new_table_name, source_table_name)

    Then("is ok")
    clickhouse.drop_table(new_table_name)


def test_swap_tables(clickhouse: ClickHouse):
    Given("tables")
    table_name = "test"
    new_table_name = "test_new"
    old_table_name = "test_old"
    source_table_name = "advertiser"

    When("create and swap")
    clickhouse.create_table_as(table_name, source_table_name)
    clickhouse.create_table_as(new_table_name, source_table_name)
    clickhouse.swap_tables(table_name, new_table_name, old_table_name)

    Then("is ok")
    clickhouse.drop_table(old_table_name)
    clickhouse.drop_table(table_name)

