-- CHECK constraints on Silver
ALTER TABLE silver_orders_cleaned
    ADD CONSTRAINT chk_orders_total_amount_nonneg CHECK (total_amount >= 0),
    ADD CONSTRAINT chk_orders_discount_nonneg CHECK (discount_amount >= 0),
    ADD CONSTRAINT chk_orders_delivery_time_range CHECK (delivery_time_mins IS NULL OR delivery_time_mins BETWEEN 0 AND 120);

ALTER TABLE silver_restaurants_cleaned
    ADD CONSTRAINT chk_restaurants_rating_range CHECK (avg_rating IS NULL OR avg_rating BETWEEN 0 AND 5),
    ADD CONSTRAINT chk_restaurants_prep_time_range CHECK (avg_prep_time_mins IS NULL OR avg_prep_time_mins BETWEEN 0 AND 120),
    ADD CONSTRAINT chk_restaurants_commission_range CHECK (commission_pct IS NULL OR commission_pct BETWEEN 0 AND 100);

ALTER TABLE silver_agents_cleaned
    ADD CONSTRAINT chk_agents_rating_range CHECK (avg_rating IS NULL OR avg_rating BETWEEN 0 AND 5);

ALTER TABLE silver_payments_cleaned
    ADD CONSTRAINT chk_payments_amount_nonneg CHECK (amount >= 0);

ALTER TABLE silver_complaints_cleaned
    ADD CONSTRAINT chk_complaints_resolution_time_nonneg CHECK (resolution_time_hrs IS NULL OR resolution_time_hrs >= 0),
    ADD CONSTRAINT chk_complaints_compensation_nonneg CHECK (compensation_amount >= 0);

-- fn_calculate_cltv
CREATE OR REPLACE FUNCTION fn_calculate_cltv(p_customer_id TEXT)
RETURNS DECIMAL(12,2)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cltv DECIMAL(12,2);
    v_customer_sk INT;
BEGIN
    SELECT customer_sk INTO v_customer_sk
    FROM gold.fact_orders
    WHERE customer_sk IS NOT NULL
    LIMIT 1;
    IF v_customer_sk IS NULL THEN
        RAISE NOTICE 'fn_calculate_cltv: no facts for %', p_customer_id;
        RETURN NULL;
    END IF;
    SELECT COALESCE(SUM(net_amount), 0) INTO v_cltv
    FROM gold.fact_orders
    WHERE cancellation_flag = FALSE;
    RETURN v_cltv;
END;
$$;

-- trg_update_updated_at
CREATE OR REPLACE FUNCTION trg_update_updated_at_fn()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := NOW();
    RETURN NEW;
END;
$$;

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'silver_orders_cleaned', 'silver_order_items_cleaned',
        'silver_restaurants_cleaned', 'silver_customers_cleaned',
        'silver_agents_cleaned', 'silver_payments_cleaned',
        'silver_complaints_cleaned'
    ]
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_update_updated_at ON %I', t);
        EXECUTE format('CREATE TRIGGER trg_update_updated_at BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION trg_update_updated_at_fn()', t);
    END LOOP;
END $$;

-- sp_rebuild_reference_dims
CREATE OR REPLACE PROCEDURE sp_rebuild_reference_dims()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE gold.dim_order_status, gold.dim_payment_status, gold.dim_complaint_category;
    INSERT INTO gold.dim_order_status (status_name, status_category, is_final_state) VALUES
        ('delivered', 'Completed', TRUE), ('cancelled', 'Cancelled', TRUE),
        ('pending', 'Active', FALSE), ('refunded', 'Cancelled', TRUE);
    INSERT INTO gold.dim_payment_status (status_name, status_category, is_final_state) VALUES
        ('success', 'Completed', TRUE), ('failed', 'Failed', TRUE), ('pending', 'Active', FALSE);
    INSERT INTO gold.dim_complaint_category (category_name, category_group) VALUES
        ('Delivery Delay', 'Logistics'), ('Wrong Item', 'Food Quality'),
        ('Quality Issue', 'Food Quality'), ('Payment Problem', 'Payment');
    RAISE NOTICE 'sp_rebuild_reference_dims: 3 dims rebuilt';
END;
$$;