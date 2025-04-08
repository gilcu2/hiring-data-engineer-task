from datetime import date, datetime, timedelta
from typing import Optional

from postgres_spark import PostgresSpark
from clickhouse_spark import ClickHouseSpark
from clickhouse import ClickHouse
from postgres import Postgres
from pyspark.sql.functions import from_utc_timestamp, col
import typer
from typing_extensions import Annotated
from spark import create_spark
from db import Extremes
from dataclasses import dataclass


@dataclass
class UpdatedRows:
    advertiser: int
    campaign: int
    clicks: int
    impressions: int


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


def update_all(
        from_date: date, to_date: date,
        postgres_spark: PostgresSpark, clickhouse_spark: ClickHouseSpark,
        ch_suffix: str = "", limit: Optional[int] = None
) -> UpdatedRows:
    n_rows_advertiser = update_entity("advertiser", f"advertiser{ch_suffix}",
                                      postgres_spark, clickhouse_spark, limit)
    n_rows_campaign = update_entity("campaign", f"campaign{ch_suffix}",
                                    postgres_spark, clickhouse_spark, limit)
    n_rows_clicks = update_kpi("clicks", "campaign", f"clicks{ch_suffix}",
                               from_date, to_date, postgres_spark, clickhouse_spark, limit)
    n_rows_impressions = update_kpi("impressions", "campaign", f"impressions{ch_suffix}",
                                    from_date, to_date, postgres_spark, clickhouse_spark, limit)
    return UpdatedRows(n_rows_advertiser, n_rows_campaign, n_rows_clicks, n_rows_impressions)


def recreate_ch_tables(clickhouse: ClickHouse, ch_suffix: str):
    tables = ["advertiser", "campaign", "clicks", "impressions"]
    if ch_suffix != "":
        for table in tables:
            clickhouse.create_table_as(f"{table}{ch_suffix}", table)


def get_update_interval(postgres: Postgres, clickhouse: ClickHouse,
                        pg_table: str = "impressions", ch_table: str = "impressions"
                        ) -> Optional[Extremes[date]]:
    yesterday = (datetime.today() - timedelta(days=1)).date()
    ch_extremes = clickhouse.get_extremes(ch_table, "created_at")
    if ch_extremes:
        if ch_extremes.max_value.date() < yesterday:
            return Extremes((ch_extremes.max_value + timedelta(days=1)).date(), yesterday)
        else:
            return None

    pg_extremes = postgres.get_extremes(pg_table, "created_at")
    if pg_extremes:
        return Extremes(pg_extremes.min_value.date(), yesterday)
    else:
        return None


def main(
        from_date: Annotated[Optional[datetime], typer.Argument(formats=["%Y-%m-%d"])]=None,
        to_date: Annotated[Optional[datetime], typer.Argument(formats=["%Y-%m-%d"])]=None,
        pg_url: str = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres",
        ch_url: str = "jdbc:ch://localhost:8123/default?user=default&password=12345",
        limit: Optional[int] = None,
        ch_suffix: str = ""

) -> UpdatedRows:
    """
        Update the data in ClickHouse from PostgreSQL.
    """
    clickhouse = ClickHouse(ch_url)
    postgres = Postgres(pg_url)

    if (from_date is None) or (to_date is None):
        pg_table = "impressions"
        ch_table = f"impressions{ch_suffix}"
        interval = get_update_interval(postgres, clickhouse, pg_table, ch_table)
        if interval is None:
            print("Nothing to update")
            return UpdatedRows(0, 0, 0, 0)
        from_date = interval.min_value
        to_date = interval.max_value
    else:
        from_date = from_date.date()
        to_date = to_date.date()

    if from_date > to_date:
        print(f"Nothing to update, from_date: {from_date} > to_date: {to_date}")
        return UpdatedRows(0, 0, 0, 0)

    print(f"Begin updating clickhouse from postgres from {from_date} to {to_date}")
    spark = create_spark("postgres to clickhouse update pipeline")

    postgres_spark = PostgresSpark(spark, pg_url)
    clickhouse_spark = ClickHouseSpark(spark, ch_url, clickhouse)

    if ch_suffix != "":
        recreate_ch_tables(clickhouse, ch_suffix)

    updated_rows = update_all(from_date, to_date, postgres_spark, clickhouse_spark, ch_suffix,
                              limit)

    print(f"Updated rows: {updated_rows}")

    return updated_rows


if __name__ == "__main__":
    typer.run(main)
