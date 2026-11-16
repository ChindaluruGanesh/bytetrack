-- sp_reconcile_pipeline
CREATE OR REPLACE PROCEDURE sp_reconcile_pipeline()
LANGUAGE plpgsql
AS $$
DECLARE
    bronze_tables TEXT[] := ARRAY[
        'stg_restaurants_raw','stg_customers_raw','stg_agents_raw',
        'stg_orders_raw','stg_order_items_raw','stg_payments_raw','stg_complaints_raw'
    ];
    silver_tables TEXT[] := ARRAY[
        'silver_restaurants_cleaned','silver_customers_cleaned','silver_agents_cleaned',
        'silver_orders_cleaned','silver_order_items_cleaned','silver_payments_cleaned','silver_complaints_cleaned'
    ];
    i INT;
    bronze_count BIGINT;
    silver_count BIGINT;
    rejected_count BIGINT;
BEGIN
    FOR i IN 1 .. array_length(bronze_tables, 1) LOOP
        EXECUTE format('SELECT COUNT(*) FROM %I', bronze_tables[i]) INTO bronze_count;
        EXECUTE format('SELECT COUNT(*) FROM %I', silver_tables[i]) INTO silver_count;
        SELECT COUNT(*) INTO rejected_count FROM (
            SELECT 1 FROM stg_rejected_level1 WHERE source_table = bronze_tables[i]
            UNION ALL
            SELECT 1 FROM stg_rejected_level2 WHERE source_table = bronze_tables[i]
        ) r;
        RAISE NOTICE '% : bronze=% silver=% rejected=% likely_deduped=%',
            rpad(bronze_tables[i], 28), bronze_count, silver_count, rejected_count,
            GREATEST(bronze_count - silver_count - rejected_count, 0);
    END LOOP;
END;
$$;

-- sp_validate_silver
CREATE OR REPLACE PROCEDURE sp_validate_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    bad_cancelled_amount INT;
    bad_discount INT;
    bad_delivery_order INT;
BEGIN
    SELECT COUNT(*) INTO bad_cancelled_amount
    FROM silver_orders_cleaned WHERE status = 'cancelled' AND total_amount <> 0;
    SELECT COUNT(*) INTO bad_discount
    FROM silver_orders_cleaned WHERE discount_amount > total_amount;
    SELECT COUNT(*) INTO bad_delivery_order
    FROM silver_orders_cleaned WHERE delivery_ts IS NOT NULL AND delivery_ts <= order_ts;
    RAISE NOTICE 'sp_validate_silver: cancelled_with_nonzero=% discount_gt_total=% delivery_before_order=%',
        bad_cancelled_amount, bad_discount, bad_delivery_order;
    IF bad_cancelled_amount > 0 OR bad_discount > 0 OR bad_delivery_order > 0 THEN
        RAISE WARNING 'Business-rule violation(s) found in Silver';
    END IF;
END;
$$;

-- sp_archive_old_data
CREATE OR REPLACE PROCEDURE sp_archive_old_data(p_cutoff_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_ids TEXT[];
BEGIN
    SELECT ARRAY_AGG(order_id) INTO v_order_ids
    FROM silver_orders_cleaned WHERE order_ts < p_cutoff_date;
    IF v_order_ids IS NULL THEN
        RAISE NOTICE 'sp_archive_old_data: nothing older than %', p_cutoff_date;
        RETURN;
    END IF;
    DELETE FROM silver_complaints_cleaned  WHERE order_id = ANY(v_order_ids);
    DELETE FROM silver_payments_cleaned    WHERE order_id = ANY(v_order_ids);
    DELETE FROM silver_order_items_cleaned WHERE order_id = ANY(v_order_ids);
    DELETE FROM silver_orders_cleaned      WHERE order_id = ANY(v_order_ids);
    RAISE NOTICE 'sp_archive_old_data: deleted % orders (and dependents) older than %',
        array_length(v_order_ids, 1), p_cutoff_date;
END;
$$;

-- fn_get_surrogate_key
CREATE OR REPLACE FUNCTION fn_get_surrogate_key(p_dim_name TEXT, p_business_key TEXT)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_sk INT;
    v_table TEXT;
    v_sk_col TEXT;
    v_bk_col TEXT;
BEGIN
    CASE p_dim_name
        WHEN 'restaurant' THEN v_table := 'gold.dim_restaurant'; v_sk_col := 'dbt_scd_id'; v_bk_col := 'restaurant_id';
        WHEN 'customer'   THEN v_table := 'gold.dim_customer';   v_sk_col := 'dbt_scd_id'; v_bk_col := 'customer_id';
        WHEN 'agent'      THEN v_table := 'gold.dim_agent';      v_sk_col := 'dbt_scd_id'; v_bk_col := 'agent_id';
        ELSE RAISE EXCEPTION 'fn_get_surrogate_key: unknown dim_name %', p_dim_name;
    END CASE;
    EXECUTE format('SELECT %I::text::int FROM %s WHERE %I = $1 AND dbt_valid_to IS NULL LIMIT 1',
        v_sk_col, v_table, v_bk_col) INTO v_sk USING p_business_key;
    RETURN v_sk;
END;
$$;

-- fn_generate_row_hash
CREATE OR REPLACE FUNCTION fn_generate_row_hash(p_input TEXT)
RETURNS TEXT
LANGUAGE sql
IMMUTABLE
AS $$
    SELECT md5(COALESCE(p_input, ''));
$$;

-- fn_get_dlq_summary
CREATE OR REPLACE FUNCTION fn_get_dlq_summary()
RETURNS TABLE(source_table VARCHAR, rejection_level INT, error_reason TEXT, occurrence_count BIGINT)
LANGUAGE sql
AS $$
    SELECT source_table, rejection_level, error_reason, COUNT(*)
    FROM (
        SELECT source_table, rejection_level, error_reason FROM stg_rejected_level1
        UNION ALL
        SELECT source_table, rejection_level, error_reason FROM stg_rejected_level2
    ) x
    GROUP BY source_table, rejection_level, error_reason
    ORDER BY COUNT(*) DESC;
$$;

-- trg_prevent_dim_delete
CREATE OR REPLACE FUNCTION trg_prevent_dim_delete_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Hard delete blocked on %: SCD2 dimensions must never be hard-deleted', TG_TABLE_NAME;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_prevent_dim_delete ON gold.dim_restaurant;
CREATE TRIGGER trg_prevent_dim_delete BEFORE DELETE ON gold.dim_restaurant
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_dim_delete_fn();
DROP TRIGGER IF EXISTS trg_prevent_dim_delete ON gold.dim_customer;
CREATE TRIGGER trg_prevent_dim_delete BEFORE DELETE ON gold.dim_customer
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_dim_delete_fn();
DROP TRIGGER IF EXISTS trg_prevent_dim_delete ON gold.dim_agent;
CREATE TRIGGER trg_prevent_dim_delete BEFORE DELETE ON gold.dim_agent
    FOR EACH ROW EXECUTE FUNCTION trg_prevent_dim_delete_fn();