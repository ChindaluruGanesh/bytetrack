{% snapshot dim_agent %}
{{ config(
    target_schema='gold',
    unique_key='agent_id',
    strategy='check',
    check_cols=['vehicle_type', 'tier', 'avg_rating', 'total_deliveries']
) }}
SELECT agent_id, name, city, vehicle_type, tier, avg_rating, total_deliveries, join_date
FROM {{ source('silver', 'silver_agents_cleaned') }}
{% endsnapshot %}
