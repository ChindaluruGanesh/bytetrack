SELECT
    payment_id,
    order_id,
    amount,
    cashback,
    payment_method,
    gateway,
    status,
    payment_ts,
    (status = 'success') AS is_successful
FROM "airflow"."gold_silver"."stg_payments"