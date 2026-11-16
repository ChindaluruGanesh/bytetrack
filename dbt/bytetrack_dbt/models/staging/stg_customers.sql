SELECT customer_id, name, city, signup_date, loyalty_tier, total_orders
FROM {{ source('silver', 'silver_customers_cleaned') }}
