WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
restaurants_with_sk AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY restaurant_id, dbt_valid_from) AS restaurant_sk,
        restaurant_id,
        COALESCE(dbt_valid_from, '1900-01-01'::timestamp) AS dbt_valid_from,
        COALESCE(dbt_valid_to, '9999-12-31'::timestamp) AS dbt_valid_to
    FROM {{ ref('dim_restaurant') }}
),
customers_with_sk AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY customer_id, dbt_valid_from) AS customer_sk,
        customer_id,
        COALESCE(dbt_valid_from, '1900-01-01'::timestamp) AS dbt_valid_from,
        COALESCE(dbt_valid_to, '9999-12-31'::timestamp) AS dbt_valid_to
    FROM {{ ref('dim_customer') }}
),
agents_with_sk AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY agent_id, dbt_valid_from) AS agent_sk,
        agent_id,
        COALESCE(dbt_valid_from, '1900-01-01'::timestamp) AS dbt_valid_from,
        COALESCE(dbt_valid_to, '9999-12-31'::timestamp) AS dbt_valid_to
    FROM {{ ref('dim_agent') }}
)
SELECT
    o.order_id,
    r.restaurant_sk,
    c.customer_sk,
    a.agent_sk,
    o.order_ts,
    o.status,
    o.total_amount,
    o.discount_amount,
    (o.total_amount - COALESCE(o.discount_amount, 0)) AS net_amount,
    o.delivery_time_mins,
    o.city,
    o.zone,
    o.cuisine_type,
    (o.delivery_time_mins > 45) AS is_late,
    (o.status = 'cancelled') AS cancellation_flag
FROM orders o
LEFT JOIN restaurants_with_sk r
    ON r.restaurant_id = o.restaurant_id
   AND o.order_ts >= r.dbt_valid_from
   AND o.order_ts < r.dbt_valid_to
LEFT JOIN customers_with_sk c
    ON c.customer_id = o.customer_id
   AND o.order_ts >= c.dbt_valid_from
   AND o.order_ts < c.dbt_valid_to
LEFT JOIN agents_with_sk a
    ON a.agent_id = o.agent_id
   AND o.order_ts >= a.dbt_valid_from
   AND o.order_ts < a.dbt_valid_to