WITH weekly AS (
    SELECT
        agent_sk,
        DATE_TRUNC('week', order_ts)::date AS week_start_date,
        COUNT(*) AS total_deliveries,
        COUNT(*) FILTER (WHERE NOT is_late) AS on_time_deliveries,
        COUNT(*) FILTER (WHERE cancellation_flag) AS cancelled_deliveries
    FROM {{ ref('fact_orders') }}
    WHERE agent_sk IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    ROW_NUMBER() OVER (ORDER BY week_start_date, agent_sk) AS report_id,
    agent_sk,
    week_start_date,
    total_deliveries,
    on_time_deliveries,
    ROUND(on_time_deliveries::numeric / NULLIF(total_deliveries, 0) * 100, 2) AS on_time_pct,
    cancelled_deliveries,
    RANK() OVER (PARTITION BY week_start_date ORDER BY total_deliveries DESC) AS rank
FROM weekly
