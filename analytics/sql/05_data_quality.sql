-- Observability: run metrics and data-quality results (ops tables).
-- The ops tables are registered by `make analytics` together with the gold tables.
SELECT batch_date, task, table_name, status, input_rows, output_rows, invalid_rows, duplicate_rows,
       duration_seconds, message
FROM ops.pipeline_metrics
ORDER BY recorded_at DESC
LIMIT 50;

SELECT batch_date, check_name, table_name, severity, passed, observed, expected
FROM ops.dq_results
WHERE NOT passed
ORDER BY checked_at DESC;
