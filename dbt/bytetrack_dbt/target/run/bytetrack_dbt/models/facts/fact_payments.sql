
  
    

  create  table "airflow"."gold"."fact_payments__dbt_tmp"
  
  
    as
  
  (
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
  );
  