
      update "airflow"."gold"."dim_agent"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "dim_agent__dbt_tmp082829873115" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "airflow"."gold"."dim_agent".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "airflow"."gold"."dim_agent".dbt_valid_to is null;

    insert into "airflow"."gold"."dim_agent" ("agent_id", "name", "city", "vehicle_type", "tier", "avg_rating", "total_deliveries", "join_date", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."agent_id",DBT_INTERNAL_SOURCE."name",DBT_INTERNAL_SOURCE."city",DBT_INTERNAL_SOURCE."vehicle_type",DBT_INTERNAL_SOURCE."tier",DBT_INTERNAL_SOURCE."avg_rating",DBT_INTERNAL_SOURCE."total_deliveries",DBT_INTERNAL_SOURCE."join_date",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "dim_agent__dbt_tmp082829873115" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  