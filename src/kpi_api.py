from fastapi import FastAPI
from starlette.responses import RedirectResponse
from typing_extensions import Optional
from clickhouse import ClickHouse
import uvicorn
import os

ch_url = os.getenv("CH_URL", "jdbc:ch://localhost:8123/default?user=default&password=12345")
clickhouse = ClickHouse(ch_url)

app = FastAPI()


@app.get("/ctr_campaign/")
def ctr_campaign(suffix: str = "", limit: Optional[int] = None):
    advertiser_table = f"advertiser{suffix}"
    campaign_table = f"campaign{suffix}"
    impressions_table = f"impressions{suffix}"
    clicks_table = f"clicks{suffix}"

    sql_limit = f" LIMIT {limit}" if limit else ""
    sql = f"""
        WITH
            c AS (
                SELECT campaign_id,  count() AS total_clicks 
                FROM {clicks_table} GROUP BY campaign_id {sql_limit}
            ),
            i AS (
                SELECT campaign_id, advertiser_id, count() AS total_impressions 
                FROM {impressions_table} GROUP BY campaign_id,advertiser_id {sql_limit}
            )
        SELECT
            a.name as advertiser,
            i.advertiser_id,
            ca.name as campaign,
            i.campaign_id,   
            i.total_impressions,
            c.total_clicks,
            round(c.total_clicks / nullIf(i.total_impressions, 0), 4) AS ctr
        FROM i
        ANY LEFT JOIN c USING (campaign_id)
        ANY LEFT JOIN {advertiser_table} a ON advertiser_id = a.id
        ANY LEFT JOIN {campaign_table} ca ON campaign_id = ca.id
        ORDER BY ctr DESC
    """
    r = clickhouse.query(sql)
    return r


@app.get("/daily_impressions/")
def daily_impressions(suffix: str = "",limit: Optional[int] = None):
    impressions_table = f"impressions{suffix}"
    sql_limit = f" LIMIT {limit}" if limit else ""
    sql = f"""
        SELECT
            toDate(i.created_at) AS day,
            count() AS impressions
        FROM {impressions_table} i
        GROUP BY day
        ORDER BY day
        {sql_limit}
    """
    r = clickhouse.query(sql)
    return r


@app.get("/daily_clicks/")
def daily_clicks(suffix: str = "",limit: Optional[int] = None):
    clicks_table = f"clicks{suffix}"
    sql_limit = f" LIMIT {limit}" if limit else ""
    sql = f"""
        SELECT
            toDate(i.created_at) AS day,
            count() AS clicks
        FROM {clicks_table} i
        GROUP BY day
        ORDER BY day
        {sql_limit}
    """
    r = clickhouse.query(sql)
    return r


@app.get("/", include_in_schema=False)
async def docs_redirect():
    return RedirectResponse(url='/docs')


if __name__ == "__main__":
    uvicorn.run("kpi_api:app", host='0.0.0.1', port=8000, reload=True)
