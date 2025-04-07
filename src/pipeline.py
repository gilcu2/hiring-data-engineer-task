from datetime import date, datetime
from typing import Optional

from postgres_spark import PostgresSpark
from clickhouse_spark import ClickHouseSpark
from clickhouse import ClickHouse
from pyspark.sql.functions import from_utc_timestamp, col
import typer
from typing_extensions import Annotated
from spark import create_spark


def update_entity(pg_table_name: str, ch_table_name: str,
                  postgres: PostgresSpark, clickhouse: ClickHouseSpark,
                  limit: Optional[int] = None
                  ) -> int:
    query = f"SELECT * FROM {pg_table_name} LIMIT {limit}" if limit else f"SELECT * FROM {pg_table_name}"
    df = postgres.read(query)
    clickhouse.replace_table_data(ch_table_name, df)
    return df.count()


def update_kpi(kpi_table_name: str, campaign_table_name: str, ch_table_name: str,
               from_date: date, to_date: date,
               postgres: PostgresSpark, clickhouse: ClickHouseSpark,
               limit: Optional[int] = None
               ) -> int:
    query0 = f"""
        SELECT 
            kpi.id as id,
            kpi.campaign_id as campaign_id,
            campaign.advertiser_id as advertiser_id,
            kpi.created_at as created_at
        FROM {kpi_table_name} kpi 
        JOIN {campaign_table_name} campaign
        ON kpi.campaign_id = campaign.id
        WHERE 
            kpi.created_at::date BETWEEN '{from_date}' AND '{to_date}'
        """
    query = f"{query0} LIMIT {limit}" if limit else query0
    df = postgres.read(query)
    df_berlin = df.withColumn("created_at", from_utc_timestamp(col("created_at"), "Europe/Berlin"))
    clickhouse.write(df_berlin, ch_table_name)
    return df.count()


def update_clickhouse(
        from_date: date, to_date: date,
        postgres: PostgresSpark, clickhouse: ClickHouseSpark,
        ch_suffix: str = "", limit: Optional[int] = None
) -> list[int]:
    n_rows_advertiser = update_entity("advertiser", f"advertiser{ch_suffix}",
                                      postgres, clickhouse, limit)
    n_rows_campaign = update_entity("campaign", f"campaign{ch_suffix}",
                                    postgres, clickhouse, limit)
    n_rows_clicks = update_kpi("clicks", "campaign", f"clicks{ch_suffix}",
                               from_date, to_date, postgres, clickhouse, limit)
    n_rows_impressions = update_kpi("impressions", "campaign", f"impressions{ch_suffix}",
                                    from_date, to_date, postgres, clickhouse, limit)
    return [n_rows_advertiser, n_rows_campaign, n_rows_clicks, n_rows_impressions]


def main(
        from_date: Annotated[datetime, typer.Argument(formats=["%Y-%m-%d"])],
        to_date: Annotated[datetime, typer.Argument(formats=["%Y-%m-%d"])],
        pg_url: str = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres",
        ch_url: str = "jdbc:ch://localhost:8123/default?user=default&password=12345",
        limit: Optional[int] = None,
        ch_suffix: str = ""

) -> list[int]:
    """
        Update the data in ClickHouse from PostgreSQL.
    """

    spark = create_spark("postgres to clickhouse update pipeline")
    clickhouse = ClickHouse(ch_url)
    postgres_spark = PostgresSpark(spark, pg_url)
    clickhouse_spark = ClickHouseSpark(spark, ch_url, clickhouse)

    tables = ["advertiser", "campaign", "clicks", "impressions"]
    for table in tables:
        clickhouse.create_table_as(f"{table}{ch_suffix}", table)

    updated_rows = update_clickhouse(from_date.date(), to_date.date(), postgres_spark, clickhouse_spark, ch_suffix,
                                     limit)

    print("Updated rows")
    for i in range(4):
        print(f"{tables[i]}: {updated_rows[i]}")

    return updated_rows


def get_needed_update_interval():
    today = datetime.today()


if __name__ == "__main__":
    typer.run(main)
