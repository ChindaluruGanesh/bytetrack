WITH monthly AS (
    SELECT
        customer_sk,
        DATE_TRUNC('month', order_ts)::date AS month,
        COUNT(*) AS orders_in_month
    FROM {{ ref('fact_orders') }}
    WHERE cancellation_flag = FALSE
    GROUP BY 1, 2
)
SELECT
    ROW_NUMBER() OVER (ORDER BY month) AS report_id,
    month,
    COUNT(*) AS total_customers,
    COUNT(*) FILTER (WHERE orders_in_month > 1) AS repeat_customers,
    COUNT(*) FILTER (WHERE orders_in_month = 1) AS one_time_customers,
    COUNT(*) FILTER (WHERE orders_in_month >= 5) AS frequent_customers,
    ROUND(COUNT(*) FILTER (WHERE orders_in_month > 1)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS repeat_rate_pct
FROM monthly
GROUP BY month
