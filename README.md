# Data Engineering Task: Reynaldo Gil

## Project requirements

* Python 3.12
* [uv](https://docs.astral.sh/uv/getting-started/installation/)
* [docker](https://docs.docker.com/engine/install/)
* [compose](https://docs.docker.com/compose/install/)

## Setup

1. uv sync
1. ./scripts/docker-build.sh
1. docker-compose up -d
1. uv run python main.py batch

## Checks

1. uv run pytest
2. uv run ruff check

## ClickHouse Schema

Available in migrations/clickhouse/1__create_schema.sql. The schema was optimized
to analytical queries by:
- Denormalizing the tables impressions and queries.
  They have additionally the advertiser id to avoid the secondary join between campaign and
  advertiser
- Partitioned by day to avoid load the data not related when queries by time range
- Using the merge tree engine, optimo for append and analytic

The clickhouse server was deployed in the docker-compose. Also the clickhouse migration and 
tabix client.

## Data pipeline

The main code in src/pipeline.py. Given that the problem only requires daily updates, 
needs data modifications, have to be done without cloud access and trying to have scalability 
the processing was implemented by a custom ETL in PySpark. Real time capabilities are not needed
so using Kafka is not necessary. Other tools don't allow data transformation or 
don't have scalability or are not optimal/easier to use for batch processing.

Spark server and worker was deployed in docker-compose.
Monitoring of the spark processing is available in http://localhost:8080/

The small tables Advertiser and Campaign are by updated by replace the data because
we think that doesn't worth doing a more complicated process. Also, we take into account that Clickhouse 
is not optimo for individual rows updating.

The big tables Impressions and Cliks are updated by appending taking advantage of their incremental nature
and the fact that click house is optimo for appending data.

### Pipeline scheduling

The pipeline scheduling was implemented with Prefect. Will run every day at 1 A.M. 
The flow and deployment are defined in src/workflow.py. Prefect server and worker are deployed 
too in docker-compose.
Monitoring of the pipeline is available at http://localhost:4200/

## KPIs

The KPIs were implemented in Fastapi. Code and queries are available in src/kpi_api.py. 
API documentation is available at http://localhost:8000/