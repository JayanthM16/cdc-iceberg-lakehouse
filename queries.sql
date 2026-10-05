-- Run:  docker compose run --rm spark-sql -f /opt/sql/queries.sql

-- 1. Current state (what a dashboard would query)
SELECT status, COUNT(*) AS orders, ROUND(SUM(amount), 2) AS revenue
FROM lake.silver.orders WHERE NOT is_deleted
GROUP BY status ORDER BY orders DESC;

-- 2. Change-event mix captured in bronze
SELECT op, COUNT(*) AS events FROM lake.bronze.orders_cdc GROUP BY op;

-- 3. Iceberg snapshots: one per micro-batch commit
SELECT snapshot_id, committed_at, operation, summary['added-records'] AS added
FROM lake.silver.orders.snapshots ORDER BY committed_at DESC LIMIT 10;

-- 4. Time travel: row count at the oldest snapshot vs now
SELECT COUNT(*) AS rows_at_first_snapshot
FROM lake.silver.orders VERSION AS OF
  (SELECT snapshot_id FROM lake.silver.orders.snapshots ORDER BY committed_at ASC LIMIT 1);

-- 5. Physical layout: file counts and sizes (small-file problem check)
SELECT COUNT(*) AS data_files, ROUND(AVG(file_size_in_bytes)/1024, 1) AS avg_kb
FROM lake.silver.orders.files;

-- 6. Table maintenance (run periodically; compacts small files, expires old snapshots)
-- CALL lake.system.rewrite_data_files(table => 'silver.orders');
-- CALL lake.system.expire_snapshots(table => 'silver.orders', retain_last => 20);

-- 7. End-to-end latency (source commit -> lakehouse ingest), milliseconds
SELECT percentile(unix_millis(ingested_at) - source_ts_ms, array(0.5, 0.95)) AS p50_p95_latency_ms,
       COUNT(*) AS events
FROM lake.bronze.orders_cdc
WHERE op <> 'r';   -- exclude initial-snapshot reads
