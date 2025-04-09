#!/bin/bash

DB_HOST=${CH_HOST:-localhost}

echo DB_HOST: $DB_HOST

clickhouse-migrations --db-host $DB_HOST --db-port 9000 \
    --db-user default \
    --db-password 12345 \
    --db-name default \
    --migrations-dir ./migrations/clickhouse \
    --log-level info