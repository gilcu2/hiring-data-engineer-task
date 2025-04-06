from datetime import date
from typing import Optional

from postgres import Postgres
from clickhouse import ClickHouse
from pyspark.sql.functions import from_utc_timestamp,col


def update_entity(pg_table_name: str, ch_table_name: str,
                  postgres: Postgres, clickhouse: ClickHouse,
                  limit: Optional[int] = None
                  ) -> int:
    query = f"SELECT * FROM {pg_table_name} LIMIT {limit}" if limit else f"SELECT * FROM {pg_table_name}"
    df = postgres.read(query)
    clickhouse.replace_table_data(ch_table_name, df)
    return df.count()


def update_kpi(kpi_table_name: str, campaign_table_name: str, ch_table_name: str,
               from_date: date, to_date: date,
               postgres: Postgres, clickhouse: ClickHouse,
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


def update_cliphouse(from_date: date, to_date: date, postgres: Postgres, clickhouse: ClickHouse):
    pg_advertiser_df = postgres.read()


if __name__ == "__main__":
    print("Hello, World!")
