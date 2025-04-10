from prefect import flow
from datetime import date
from typing import Optional
from postgres import Postgres
from clickhouse import ClickHouse
from postgres_spark import PostgresSpark
from clickhouse_spark import ClickHouseSpark
from pipeline import update_all, get_update_interval, UpdatedRows, recreate_ch_tables
from spark import create_spark
import os
from prefect.settings import PREFECT_API_URL

ch_url = os.getenv("CH_URL", "jdbc:ch://localhost:8123/default?user=default&password=12345")
pg_url = os.getenv("PG_URL", "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres")
print(f"PREFECT_API_URL: {PREFECT_API_URL.value()}")

@flow(log_prints=True)
def update_flow(from_date: Optional[date] = None, to_date: Optional[date] = None,
                ch_suffix: str = "", limit: Optional[int] = None) -> UpdatedRows:
    print(f"Begin updating clickhouse from postgres from {from_date} to {to_date} {ch_suffix} {limit}")
    postgres = Postgres(pg_url)
    clickhouse = ClickHouse(ch_url)

    if ch_suffix != "":
        recreate_ch_tables(clickhouse, ch_suffix)

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

    updated_rows = update_all(from_date, to_date, postgres_spark, clickhouse_spark, ch_suffix, limit)

    print(f"End updating clickhouse from postgres: {updated_rows}")
    return updated_rows


def get_bool_env(var_name, default=True):
    value = os.getenv(var_name)
    if value is None:
        return default
    return value.lower() in ("1", "true", "yes", "on")


def main(from_date: Optional[date] = None, to_date: Optional[date] = None,
         suffix: str = "", limit: Optional[int] = None):
    cron = os.getenv("UPDATE_CRONTAB", "* 1 * * *")
    update_now = get_bool_env("UPDATE_NOW", False)
    update_cron = get_bool_env("UPDATE_CRON", True)
    if update_cron:
        update_flow.deploy(name=f"update-flow-deployment{suffix}", cron=cron,
                           work_pool_name="default-agent-pool",
                          parameters=dict(ch_suffix=suffix, limit=limit),
                           concurrency_limit=1
                           )
    if update_now:
        update_flow(ch_suffix=suffix, limit=limit)


if __name__ == "__main__":
    main()
