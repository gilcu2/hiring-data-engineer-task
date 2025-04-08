from pendulum import interval
from prefect import flow
from datetime import datetime, timedelta, date
from typing import Optional
from postgres import Postgres
from clickhouse import ClickHouse
from postgres_spark import PostgresSpark
from clickhouse_spark import ClickHouseSpark
from pipeline import update_clickhouse, get_update_interval, UpdatedRows


@flow(log_prints=True)
def update_flow(from_date: date, to_date: date,
                pg_url: str = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres",
                ch_url: str = "jdbc:ch://localhost:8123/default?user=default&password=12345",
                ch_suffix: str = "", limit: Optional[int] = None) -> UpdatedRows:
    print(f"Begin updating clickhouse from postgres from {from_date} to {to_date}")
    postgres = Postgres(pg_url)
    clickhouse = ClickHouse(ch_url)
    pg_table = "impressions"
    ch_table = f"impressions{ch_suffix}"
    if (from_date is None) or (to_date is None):
        interval = get_update_interval(postgres, clickhouse, pg_table, ch_table)
        if interval is None:
            print("Nothing to update")
            return UpdatedRows(0, 0, 0, 0)
        from_date = interval.min_value
        to_date = interval.max_value

    if from_date > to_date:
        print(f"Nothing to update, from_date: {from_date} > to_date: {to_date}")
        return UpdatedRows(0, 0, 0, 0)

    postgres = PostgresSpark(pg_url)
    clickhouse = ClickHouseSpark(ch_url)
    updated_rows = update_clickhouse(from_date, to_date, postgres, clickhouse, ch_suffix, limit)

    print(f"End updating clickhouse from postgres: {updated_rows}")
    return updated_rows


if __name__ == "__main__":
    update_flow(None, None)
