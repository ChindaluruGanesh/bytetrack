SELECT
    ROW_NUMBER() OVER (ORDER BY source_table, error_reason) AS report_id,
    source_table,
    error_reason,
    COUNT(*) AS occurrence_count
FROM (
    SELECT source_table, error_reason FROM "airflow"."public"."stg_rejected_level1"
    UNION ALL
    SELECT source_table, error_reason FROM "airflow"."public"."stg_rejected_level2"
) x
GROUP BY source_table, error_reason