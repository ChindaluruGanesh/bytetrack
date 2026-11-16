SELECT agent_id, name, city, vehicle_type, tier,
       avg_rating, total_deliveries, join_date
FROM "airflow"."public"."silver_agents_cleaned"