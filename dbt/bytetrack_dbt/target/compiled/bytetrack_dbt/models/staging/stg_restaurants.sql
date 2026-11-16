SELECT restaurant_id, name, city, zone, cuisine_type,
       avg_rating, avg_prep_time_mins, tier,
       join_date, is_active, commission_pct
FROM "airflow"."public"."silver_restaurants_cleaned"