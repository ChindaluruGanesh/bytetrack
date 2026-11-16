
  create view "airflow"."gold_silver"."stg_complaints__dbt_tmp"
    
    
  as (
    SELECT complaint_id, order_id, customer_id, complaint_category,
       complaint_subcategory, description, resolution_time_hrs,
       compensation_amount, complaint_ts, is_resolved
FROM "airflow"."public"."silver_complaints_cleaned"
  );