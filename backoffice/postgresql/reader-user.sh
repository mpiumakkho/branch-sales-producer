#!/bin/bash
# Demo branch database only: the producer's login, which may read the four back-office tables and nothing else.
# Runs once, when the data volume is empty (docker-entrypoint-initdb.d), after schema.sql.
set -euo pipefail
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v password="$BRANCH_READER_PASSWORD" <<'SQL'
create role branch_sales_reader login password :'password';
grant select on daily_sales, daily_sales_line, daily_return, daily_return_line to branch_sales_reader;
SQL
