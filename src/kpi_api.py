from typing import Union

from fastapi import FastAPI
from typing_extensions import Annotated, Optional
import typer
from datetime import date
from clickhouse import ClickHouse
import uvicorn
import os

ch_url = os.getenv("CH_URL", "jdbc:ch://localhost:8123/default?user=default&password=12345")
clickhouse = ClickHouse(ch_url)

app = FastAPI()


# TODO Remove parameters if not implemented
@app.get("/ctr_campaign/")
def ctr_campaign(from_date: Optional[date] = None, to_date: Optional[date] = None):
    sql = """
        WITH
            c AS (SELECT campaign_id,  count() AS total_clicks FROM clicks GROUP BY campaign_id),
            i AS (SELECT campaign_id, advertiser_id, count() AS total_impressions FROM impressions GROUP BY campaign_id,advertiser_id)
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
        ANY LEFT JOIN advertiser a ON advertiser_id = a.id
        ANY LEFT JOIN campaign  ca ON campaign_id = ca.id
        ORDER BY ctr DESC
    """
    r = clickhouse.query(sql)
    return r


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


if __name__ == "__main__":
    uvicorn.run("app:app", host='127.0.0.1', port=8000, reload=True)
