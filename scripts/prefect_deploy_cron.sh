#!/bin/sh

PG_URL=${PG_URL:-"jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres"}
CH_URL=${CH_URL:-"jdbc:ch://localhost:8123/default?user=default&password=12345"}
SPARK_MASTER_URL=${SPARK_MASTER_URL:-"spark://localhost:7077"}
CRONTAB=${CRONTAB:-"* 1 * * *"}
PREFECT_API_URL=${PREFECT_API_URL:-"http://localhost:4200/api"}

prefect deployment build src/workflow.py:update_flow \
  --name "update-flow" \
  --cron $CRONTAB \
  --timezone ""Europe/Berlin"" \
  --env PG_URL=$PG_URL \
  --env CH_URL=$CH_URL \
  --env SPARK_MASTER_URL=$SPARK_MASTER_URL \
  --apply

