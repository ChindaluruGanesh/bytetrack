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
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()', t);
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()', t);
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS batch_id VARCHAR(100)', t);
        EXECUTE format('ALTER TABLE %I ADD COLUMN IF NOT EXISTS source_system VARCHAR(20) DEFAULT ''OMS''', t);
    END LOOP;
END $$;

ALTER TABLE silver_payments_cleaned   ALTER COLUMN source_system SET DEFAULT 'PGW';
ALTER TABLE silver_complaints_cleaned ALTER COLUMN source_system SET DEFAULT 'CRM';

ALTER TABLE audit_pipeline_runs ADD COLUMN IF NOT EXISTS duration_sec INT;
ALTER TABLE audit_pipeline_runs ADD COLUMN IF NOT EXISTS executed_by VARCHAR(100) DEFAULT CURRENT_USER;