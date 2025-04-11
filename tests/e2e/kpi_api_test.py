from bdd_helper import Given, When, Then
from fastapi.testclient import TestClient
from kpi_api import app
from clickhouse import ClickHouse

client = TestClient(app)


def recreate_tables_with_data(ch: ClickHouse, suffix: str = ""):
    tables = ["advertiser", "campaign", "clicks", "impressions"]
    for table in tables:
        table_name = f"{table}{suffix}"
        ch.drop_table(table_name)
        ch.create_table_as(table_name, table)

    ch.command(f"""
        INSERT INTO advertiser{suffix} (id,name, updated_at)
        VALUES 
            (1,'Advertiser A', '2025-01-01 00:00:01')
    """)

    ch.command(f"""
            INSERT INTO campaign{suffix} (id,name, updated_at)
            VALUES 
            (1, 'Campaign A', '2025-01-01 00:00:02'),
            (2, 'Campaign B', '2025-01-02 00:00:02')  
        """)

    ch.command(f"""
                INSERT INTO impressions{suffix} (id,advertiser_id,campaign_id,created_at)
                VALUES 
                    (1,1,1, '2025-01-01 00:00:03'),
                    (2,1,1, '2025-01-02 00:00:03'),
                    (3,1,1, '2025-01-03 00:00:03'),
                    (4,1,2, '2025-01-03 00:00:03')   
            """)

    ch.command(f"""
                    INSERT INTO clicks{suffix} (id,advertiser_id,campaign_id,created_at)
                    VALUES 
                        (1,1,1, '2025-01-01 00:00:03'),
                        (2,1,1, '2025-01-03 00:00:03'),
                        (3,1,2, '2025-01-03 00:00:03')
                """)


def test_ctr_campaign(clickhouse: ClickHouse):
    Given("path and tables")
    path = "/ctr_campaign"
    suffix = "_test"
    recreate_tables_with_data(clickhouse, suffix)

    When("call")
    response = client.get(path, params={"suffix": suffix})

    Then("is expected")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == 2


def test_daily_impressions(clickhouse: ClickHouse):
    Given("path")
    path = "/daily_impressions"
    suffix = "_test"
    recreate_tables_with_data(clickhouse, suffix)

    When("call")
    response = client.get(path, params={"suffix": suffix})

    Then("is expected")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == 3


def test_daily_clicks(clickhouse: ClickHouse):
    Given("path")
    path = "/daily_clicks"
    suffix = "_test"
    recreate_tables_with_data(clickhouse, suffix)

    When("call")
    response = client.get(path, params={"suffix": suffix})

    Then("is expected")
    assert response.status_code == 200
    result = response.json()
    assert len(result) == 2
