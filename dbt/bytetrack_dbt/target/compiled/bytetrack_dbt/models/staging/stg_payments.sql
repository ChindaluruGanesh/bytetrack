SELECT payment_id, order_id, amount, payment_method,
       gateway, status, payment_ts, cashback
FROM "airflow"."public"."silver_payments_cleaned"