#!/usr/bin/env bash
# Runs a SQL file in a demo branch database, standing in for the back-office system.
#   demo/sql.sh BR0001 demo/BR0001/01-enter-and-confirm.sql
#   demo/sql.sh BR0001 demo/branch-status.sql
set -euo pipefail
branch=$1
file="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"   # resolve before changing directory
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml -f "demo/$branch.compose.yaml" exec -T branch-db \
  psql -U branch_app -d branch -v ON_ERROR_STOP=1 -q < "$file"
