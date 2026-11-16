
      update "airflow"."gold"."dim_restaurant"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "dim_restaurant__dbt_tmp082829923354" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "airflow"."gold"."dim_restaurant".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "airflow"."gold"."dim_restaurant".dbt_valid_to is null;

    insert into "airflow"."gold"."dim_restaurant" ("restaurant_id", "name", "city", "zone", "cuisine_type", "avg_rating", "avg_prep_time_mins", "tier", "join_date", "is_active", "commission_pct", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."restaurant_id",DBT_INTERNAL_SOURCE."name",DBT_INTERNAL_SOURCE."city",DBT_INTERNAL_SOURCE."zone",DBT_INTERNAL_SOURCE."cuisine_type",DBT_INTERNAL_SOURCE."avg_rating",DBT_INTERNAL_SOURCE."avg_prep_time_mins",DBT_INTERNAL_SOURCE."tier",DBT_INTERNAL_SOURCE."join_date",DBT_INTERNAL_SOURCE."is_active",DBT_INTERNAL_SOURCE."commission_pct",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "dim_restaurant__dbt_tmp082829923354" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  