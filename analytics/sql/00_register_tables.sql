-- Tables are registered automatically by `make analytics` (trino/register_tables.sh).
-- Equivalent SQL, for reference or to run by hand in the Trino CLI:
--   docker compose exec trino trino
CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 'local:///lake/gold');
CREATE SCHEMA IF NOT EXISTS delta.ops  WITH (location = 'local:///lake/ops');

CALL delta.system.register_table(schema_name => 'gold', table_name => 'fact_sales',          table_location => 'local:///lake/gold/fact_sales');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_customer',        table_location => 'local:///lake/gold/dim_customer');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'dim_product',         table_location => 'local:///lake/gold/dim_product');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'agg_daily_sales',     table_location => 'local:///lake/gold/agg_daily_sales');
CALL delta.system.register_table(schema_name => 'gold', table_name => 'agg_customer_orders', table_location => 'local:///lake/gold/agg_customer_orders');
CALL delta.system.register_table(schema_name => 'ops',  table_name => 'pipeline_metrics',    table_location => 'local:///lake/ops/pipeline_metrics');
CALL delta.system.register_table(schema_name => 'ops',  table_name => 'dq_results',          table_location => 'local:///lake/ops/dq_results');
