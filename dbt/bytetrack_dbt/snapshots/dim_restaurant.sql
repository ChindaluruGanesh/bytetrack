{% snapshot dim_restaurant %}
{{ config(
    target_schema='gold',
    unique_key='restaurant_id',
    strategy='check',
    check_cols=['city', 'zone', 'cuisine_type', 'avg_rating', 'avg_prep_time_mins', 'tier', 'commission_pct', 'is_active']
) }}
SELECT restaurant_id, name, city, zone, cuisine_type,
       avg_rating, avg_prep_time_mins, tier, join_date, is_active, commission_pct
FROM {{ source('silver', 'silver_restaurants_cleaned') }}
{% endsnapshot %}
