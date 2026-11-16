
  
    

  create  table "airflow"."gold"."rpt_revenue_by_city__dbt_tmp"
  
  
    as
  
  (
    SELECT
    ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('day', order_ts), city) AS report_id,
    DATE_TRUNC('day', order_ts)::date AS report_date,
    city,
    cuisine_type,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS gross_revenue,
    SUM(discount_amount) AS discount_given,
    SUM(net_amount) AS net_revenue
FROM "airflow"."gold"."fact_orders"
GROUP BY 1 = 1, DATE_TRUNC('day', order_ts), city, cuisine_type
  );
  