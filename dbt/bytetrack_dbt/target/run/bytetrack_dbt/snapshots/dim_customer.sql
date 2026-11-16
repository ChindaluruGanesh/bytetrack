
      update "airflow"."gold"."dim_customer"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "dim_customer__dbt_tmp082829878376" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "airflow"."gold"."dim_customer".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      and "airflow"."gold"."dim_customer".dbt_valid_to is null;

    insert into "airflow"."gold"."dim_customer" ("customer_id", "name", "city", "signup_date", "loyalty_tier", "total_orders", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."customer_id",DBT_INTERNAL_SOURCE."name",DBT_INTERNAL_SOURCE."city",DBT_INTERNAL_SOURCE."signup_date",DBT_INTERNAL_SOURCE."loyalty_tier",DBT_INTERNAL_SOURCE."total_orders",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "dim_customer__dbt_tmp082829878376" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  