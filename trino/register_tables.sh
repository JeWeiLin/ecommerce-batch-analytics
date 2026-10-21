#!/bin/sh
# Register the Delta tables written by Spark in Trino's catalog `delta`.
# Safe to run repeatedly: tables already registered, or not built yet, are skipped.
# Run inside the trino container:  docker compose exec trino sh /etc/trino/scripts/register_tables.sh
set -e
SERVER="${TRINO_SERVER:-http://localhost:8080}"
LAKE=/lakehouse/lake
q() { trino --server "$SERVER" --execute "$1"; }

i=0
until q "SELECT 1" >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -gt 60 ] && { echo "Trino did not start"; exit 1; }
  echo "waiting for Trino..."; sleep 3
done

register() {  # schema table
  if [ ! -d "$LAKE/$1/$2/_delta_log" ]; then
    echo "skip   $1.$2 (not built yet - run the pipeline first)"; return
  fi
  if [ -n "$(q "SHOW TABLES FROM delta.$1 LIKE '$2'")" ]; then
    echo "exists $1.$2"; return
  fi
  q "CALL delta.system.register_table(schema_name => '$1', table_name => '$2', table_location => 'local:///lake/$1/$2')" >/dev/null 2>&1
  echo "added  $1.$2"
}

for schema in gold ops; do
  q "CREATE SCHEMA IF NOT EXISTS delta.$schema WITH (location = 'local:///lake/$schema')" >/dev/null 2>&1
done
for t in fact_sales dim_customer dim_product agg_daily_sales agg_customer_orders; do register gold "$t"; done
for t in pipeline_metrics dq_results; do register ops "$t"; done
