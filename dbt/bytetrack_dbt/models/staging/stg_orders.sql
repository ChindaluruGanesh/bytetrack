SELECT order_id, customer_id, restaurant_id, agent_id,
       order_ts, delivery_ts, status,
       total_amount, discount_amount,
       city, zone, cuisine_type, delivery_time_mins
FROM {{ source('silver', 'silver_orders_cleaned') }}
