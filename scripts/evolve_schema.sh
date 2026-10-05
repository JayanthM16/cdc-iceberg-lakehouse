#!/usr/bin/env bash
# Demonstrates schema evolution while the stream keeps running.
# Order matters: widen the sink first, then the source, so no value is dropped from silver.
set -euo pipefail
echo "1) Add column to the Iceberg silver table (metadata-only, no data rewrite)"
docker compose run --rm spark-sql -e "ALTER TABLE lake.silver.orders ADD COLUMN channel STRING;"
echo "2) Add column in PostgreSQL (source)"
docker compose exec -T postgres psql -U app -d shop \
  -c "ALTER TABLE public.orders ADD COLUMN channel TEXT DEFAULT 'web';"
echo "3) New change events carry 'channel'; the merge picks it up automatically."
echo "   Older rows stay NULL until they are updated. Generate traffic with:"
echo "   docker compose run --rm generator"
