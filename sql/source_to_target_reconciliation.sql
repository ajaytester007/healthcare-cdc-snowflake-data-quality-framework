-- source_to_target_reconciliation.sql
-- Representative reconciliation patterns for CDC-fed Snowflake pipelines.
-- Replace source_* and target_* with actual schemas/tables.

WITH source_base AS (
    SELECT
        member_id,
        claim_id,
        CAST(last_update_ts AS TIMESTAMP) AS last_update_ts,
        amount,
        status_code
    FROM source_claims
),
target_base AS (
    SELECT
        member_id,
        claim_id,
        CAST(last_update_ts AS TIMESTAMP) AS last_update_ts,
        amount,
        status_code
    FROM target_claims
),
row_count_check AS (
    SELECT 'row_count' AS check_name,
           (SELECT COUNT(*) FROM source_base) AS source_count,
           (SELECT COUNT(*) FROM target_base) AS target_count
),
duplicate_check AS (
    SELECT 'duplicate_claims' AS check_name,
           COUNT(*) AS duplicate_rows
    FROM (
        SELECT claim_id
        FROM target_base
        GROUP BY claim_id
        HAVING COUNT(*) > 1
    )
),
null_check AS (
    SELECT 'null_business_keys' AS check_name,
           COUNT(*) AS null_rows
    FROM target_base
    WHERE member_id IS NULL OR claim_id IS NULL
),
amount_checksum AS (
    SELECT 'amount_checksum' AS check_name,
           SUM(COALESCE(amount,0)) AS source_amount_sum,
           (SELECT SUM(COALESCE(amount,0)) FROM target_base) AS target_amount_sum
    FROM source_base
),
latest_record_check AS (
    SELECT 'latest_record_mismatch' AS check_name,
           COUNT(*) AS mismatch_rows
    FROM source_base s
    FULL OUTER JOIN target_base t
      ON s.claim_id = t.claim_id
    WHERE COALESCE(s.status_code,'~') <> COALESCE(t.status_code,'~')
       OR COALESCE(s.amount, -999999) <> COALESCE(t.amount, -999999)
)
SELECT * FROM row_count_check
UNION ALL
SELECT check_name, duplicate_rows, NULL FROM duplicate_check
UNION ALL
SELECT check_name, null_rows, NULL FROM null_check
UNION ALL
SELECT check_name, source_amount_sum, target_amount_sum FROM amount_checksum
UNION ALL
SELECT check_name, mismatch_rows, NULL FROM latest_record_check;
