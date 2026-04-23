-- cdc_watermark_latency_checks.sql
-- Representative latency and replay monitoring for CDC landing tables.

WITH batch_stats AS (
    SELECT
        source_table,
        MAX(extract_ts) AS max_extract_ts,
        MAX(load_ts) AS max_load_ts,
        DATEDIFF('minute', MAX(extract_ts), MAX(load_ts)) AS latency_minutes,
        COUNT(*) AS rows_loaded
    FROM cdc_landing_claims
    GROUP BY source_table
),
late_arrivals AS (
    SELECT
        source_table,
        COUNT(*) AS late_arrival_rows
    FROM cdc_landing_claims
    WHERE load_ts > DATEADD('hour', 4, extract_ts)
    GROUP BY source_table
),
replay_detection AS (
    SELECT
        source_table,
        business_key,
        change_seq,
        COUNT(*) AS duplicate_events
    FROM cdc_landing_claims
    GROUP BY source_table, business_key, change_seq
    HAVING COUNT(*) > 1
)
SELECT
    b.source_table,
    b.max_extract_ts,
    b.max_load_ts,
    b.latency_minutes,
    b.rows_loaded,
    COALESCE(l.late_arrival_rows, 0) AS late_arrival_rows
FROM batch_stats b
LEFT JOIN late_arrivals l
  ON b.source_table = l.source_table
ORDER BY latency_minutes DESC;

-- Investigate replay / duplicate events separately
SELECT * FROM replay_detection ORDER BY duplicate_events DESC;
