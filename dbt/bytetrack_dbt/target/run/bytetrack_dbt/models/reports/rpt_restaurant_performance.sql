
  
    

  create  table "airflow"."gold"."rpt_restaurant_performance__dbt_tmp"
  
  
    as
  
  (
    WITH weekly AS (
    SELECT
        restaurant_sk,
        DATE_TRUNC('week', order_ts)::date AS week_start_date,
        COUNT(*) AS total_orders,
        COUNT(*) FILTER (WHERE status = 'delivered') AS completed_orders,
        COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_orders,
        SUM(net_amount) AS total_revenue
    FROM "airflow"."gold"."fact_orders"
    GROUP BY 1, 2
)
SELECT
    ROW_NUMBER() OVER (ORDER BY week_start_date, restaurant_sk) AS report_id,
    restaurant_sk,
    week_start_date,
    total_orders,
    completed_orders,
    cancelled_orders,
    total_revenue,
    ROUND(cancelled_orders::numeric / NULLIF(total_orders, 0) * 100, 2) AS cancellation_rate,
    RANK() OVER (PARTITION BY week_start_date ORDER BY total_revenue DESC) AS rank
FROM weekly
  );
  