{% snapshot dim_customer %}
{{ config(
    target_schema='gold',
    unique_key='customer_id',
    strategy='check',
    check_cols=['loyalty_tier', 'total_orders', 'city']
) }}
SELECT customer_id, name, city, signup_date, loyalty_tier, total_orders
FROM {{ source('silver', 'silver_customers_cleaned') }}
{% endsnapshot %}
