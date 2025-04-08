from pendulum import interval
from prefect import flow
from datetime import datetime, timedelta, date
from typing import Optional
from postgres import Postgres
from clickhouse import ClickHouse
from postgres_spark import PostgresSpark
from clickhouse_spark import ClickHouseSpark
from pipeline import update_all, get_update_interval, UpdatedRows, recreate_ch_tables
from spark import create_spark


@flow(log_prints=True)
def update_flow(from_date: Optional[date] = None, to_date: Optional[date] = None,
                pg_url: str = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres",
                ch_url: str = "jdbc:ch://localhost:8123/default?user=default&password=12345",
                ch_suffix: str = "", limit: Optional[int] = None) -> UpdatedRows:
    print(f"Begin updating clickhouse from postgres from {from_date} to {to_date}")
    postgres = Postgres(pg_url)
    clickhouse = ClickHouse(ch_url)

    if (from_date is None) or (to_date is None):
        pg_table = "impressions"
        ch_table = f"impressions{ch_suffix}"
        interval = get_update_interval(postgres, clickhouse, pg_table, ch_table)
        if interval is None:
            print("Nothing to update")
            return UpdatedRows(0, 0, 0, 0)
        from_date = interval.min_value
        to_date = interval.max_value

    if from_date > to_date:
        print(f"Nothing to update, from_date: {from_date} > to_date: {to_date}")
        return UpdatedRows(0, 0, 0, 0)

    spark = create_spark("postgres to clickhouse update pipeline")
    postgres_spark = PostgresSpark(spark, pg_url)
    clickhouse_spark = ClickHouseSpark(spark, ch_url, clickhouse)

    if ch_suffix != "":
        recreate_ch_tables(clickhouse, ch_suffix)

    updated_rows = update_all(from_date, to_date, postgres_spark, clickhouse_spark, ch_suffix, limit)

    print(f"End updating clickhouse from postgres: {updated_rows}")
    return updated_rows


if __name__ == "__main__":
    update_flow(None, None)
