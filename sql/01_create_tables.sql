CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze (7 tables)
CREATE TABLE stg_orders_raw (
    order_id VARCHAR(100), customer_id VARCHAR(100), restaurant_id VARCHAR(100),
    agent_id VARCHAR(100), order_ts VARCHAR(100), delivery_ts VARCHAR(100),
    status VARCHAR(50), total_amount VARCHAR(50), discount_amount VARCHAR(50),
    promo_code VARCHAR(50), city VARCHAR(100), zone VARCHAR(50),
    payment_method VARCHAR(50), cuisine_type VARCHAR(100),
    delivery_time_mins VARCHAR(50), ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE stg_order_items_raw (
    order_item_id VARCHAR(100), order_id VARCHAR(100), product_id VARCHAR(100),
    product_name VARCHAR(200), quantity VARCHAR(50), unit_price VARCHAR(50),
    total_price VARCHAR(50), discount VARCHAR(50), ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE stg_restaurants_raw (
    restaurant_id VARCHAR(100), name VARCHAR(200), city VARCHAR(100), zone VARCHAR(50),
    cuisine_type VARCHAR(100), avg_rating VARCHAR(50), avg_prep_time_mins VARCHAR(50),
    tier VARCHAR(50), join_date VARCHAR(50), is_active VARCHAR(20),
    commission_pct VARCHAR(50), ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE stg_customers_raw (
    customer_id VARCHAR(100), name VARCHAR(200), phone VARCHAR(50), email VARCHAR(200),
    city VARCHAR(100), signup_date VARCHAR(50), loyalty_tier VARCHAR(50),
    total_orders VARCHAR(50), ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE stg_agents_raw (
    agent_id VARCHAR(100), name VARCHAR(200), phone VARCHAR(50), city VARCHAR(100),
    vehicle_type VARCHAR(50), join_date VARCHAR(50), tier VARCHAR(50),
    avg_rating VARCHAR(50), total_deliveries VARCHAR(50), ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE stg_payments_raw (
    payment_id VARCHAR(100), order_id VARCHAR(100), amount VARCHAR(50),
    payment_method VARCHAR(50), gateway VARCHAR(50), status VARCHAR(50),
    payment_ts VARCHAR(100), cashback VARCHAR(50), ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE stg_complaints_raw (
    complaint_id VARCHAR(100), order_id VARCHAR(100), customer_id VARCHAR(100),
    complaint_category VARCHAR(100), complaint_subcategory VARCHAR(100), description TEXT,
    resolution_time_hrs VARCHAR(50), compensation_amount VARCHAR(50),
    complaint_ts VARCHAR(100), is_resolved VARCHAR(20), ingestion_ts TIMESTAMP DEFAULT NOW()
);

-- Silver (7 tables)
CREATE TABLE silver_orders_cleaned (
    order_id VARCHAR(100) PRIMARY KEY, customer_id VARCHAR(100) NOT NULL,
    restaurant_id VARCHAR(100) NOT NULL, agent_id VARCHAR(100),
    order_ts TIMESTAMP NOT NULL, delivery_ts TIMESTAMP, status VARCHAR(50) NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL, discount_amount DECIMAL(12,2) DEFAULT 0,
    promo_code VARCHAR(50), city VARCHAR(100), zone VARCHAR(50),
    payment_method VARCHAR(50), cuisine_type VARCHAR(100),
    delivery_time_mins INT, ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver_order_items_cleaned (
    order_item_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100) NOT NULL,
    product_id VARCHAR(100), product_name VARCHAR(200), quantity INT,
    unit_price DECIMAL(12,2), total_price DECIMAL(12,2), discount DECIMAL(12,2) DEFAULT 0,
    ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver_restaurants_cleaned (
    restaurant_id VARCHAR(100) PRIMARY KEY, name VARCHAR(200),
    city VARCHAR(100) NOT NULL, zone VARCHAR(50), cuisine_type VARCHAR(100) NOT NULL,
    avg_rating DECIMAL(3,2), avg_prep_time_mins INT, tier VARCHAR(50),
    join_date DATE, is_active BOOLEAN DEFAULT TRUE, commission_pct DECIMAL(5,2),
    ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver_customers_cleaned (
    customer_id VARCHAR(100) PRIMARY KEY, name VARCHAR(200), phone VARCHAR(50),
    email VARCHAR(200), city VARCHAR(100), signup_date DATE,
    loyalty_tier VARCHAR(50) NOT NULL, total_orders INT DEFAULT 0,
    ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver_agents_cleaned (
    agent_id VARCHAR(100) PRIMARY KEY, name VARCHAR(200), phone VARCHAR(50),
    city VARCHAR(100), vehicle_type VARCHAR(50) NOT NULL, join_date DATE,
    tier VARCHAR(50) NOT NULL, avg_rating DECIMAL(3,2), total_deliveries INT DEFAULT 0,
    ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver_payments_cleaned (
    payment_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100) NOT NULL,
    amount DECIMAL(12,2) NOT NULL, payment_method VARCHAR(50), gateway VARCHAR(50),
    status VARCHAR(50) NOT NULL, payment_ts TIMESTAMP NOT NULL,
    cashback DECIMAL(12,2) DEFAULT 0, ingestion_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver_complaints_cleaned (
    complaint_id VARCHAR(100) PRIMARY KEY, order_id VARCHAR(100) NOT NULL,
    customer_id VARCHAR(100) NOT NULL, complaint_category VARCHAR(100) NOT NULL,
    complaint_subcategory VARCHAR(100), description TEXT,
    resolution_time_hrs DECIMAL(5,2), compensation_amount DECIMAL(12,2) DEFAULT 0,
    complaint_ts TIMESTAMP NOT NULL, is_resolved BOOLEAN DEFAULT FALSE,
    ingestion_ts TIMESTAMP DEFAULT NOW()
);

-- Gold static dims (3)
CREATE TABLE gold.dim_date (
    date_id SERIAL PRIMARY KEY, date DATE NOT NULL UNIQUE, year INT, month INT,
    day INT, is_weekend BOOLEAN DEFAULT FALSE
);
CREATE TABLE gold.dim_time (
    time_id SERIAL PRIMARY KEY, hour_24 INT NOT NULL, minute INT NOT NULL,
    part_of_day VARCHAR(15), is_peak_hour BOOLEAN DEFAULT FALSE
);
CREATE TABLE gold.dim_payment_method (
    payment_method_id SERIAL PRIMARY KEY, method_name VARCHAR(50) NOT NULL,
    is_digital BOOLEAN DEFAULT TRUE
);

-- Safety net (3)
CREATE TABLE stg_rejected_level1 (
    id SERIAL PRIMARY KEY, source_table VARCHAR(100), raw_data JSONB,
    error_reason TEXT, rejection_level INT DEFAULT 1, rejected_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE stg_rejected_level2 (
    id SERIAL PRIMARY KEY, source_table VARCHAR(100), raw_data JSONB,
    error_reason TEXT, rejection_level INT DEFAULT 2, rejected_ts TIMESTAMP DEFAULT NOW()
);
CREATE TABLE audit_pipeline_runs (
    run_id SERIAL PRIMARY KEY, dag_run_id VARCHAR(100), table_name VARCHAR(100),
    rows_read INT, rows_inserted INT, rows_updated INT, rows_rejected INT,
    status VARCHAR(20), start_time TIMESTAMP, end_time TIMESTAMP, error_message TEXT
);

-- FKs (added after all tables exist)
ALTER TABLE silver_orders_cleaned
    ADD CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES silver_customers_cleaned(customer_id),
    ADD CONSTRAINT fk_orders_restaurant FOREIGN KEY (restaurant_id) REFERENCES silver_restaurants_cleaned(restaurant_id),
    ADD CONSTRAINT fk_orders_agent FOREIGN KEY (agent_id) REFERENCES silver_agents_cleaned(agent_id);
ALTER TABLE silver_order_items_cleaned
    ADD CONSTRAINT fk_order_items_order FOREIGN KEY (order_id) REFERENCES silver_orders_cleaned(order_id);
ALTER TABLE silver_payments_cleaned
    ADD CONSTRAINT fk_payments_order FOREIGN KEY (order_id) REFERENCES silver_orders_cleaned(order_id);
ALTER TABLE silver_complaints_cleaned
    ADD CONSTRAINT fk_complaints_order FOREIGN KEY (order_id) REFERENCES silver_orders_cleaned(order_id),
    ADD CONSTRAINT fk_complaints_customer FOREIGN KEY (customer_id) REFERENCES silver_customers_cleaned(customer_id);

-- Indexes on FK + join columns
CREATE INDEX idx_silver_orders_customer ON silver_orders_cleaned(customer_id);
CREATE INDEX idx_silver_orders_restaurant ON silver_orders_cleaned(restaurant_id);
CREATE INDEX idx_silver_orders_agent ON silver_orders_cleaned(agent_id);
CREATE INDEX idx_silver_orders_order_ts ON silver_orders_cleaned(order_ts);
CREATE INDEX idx_silver_order_items_order ON silver_order_items_cleaned(order_id);
CREATE INDEX idx_silver_payments_order ON silver_payments_cleaned(order_id);
CREATE INDEX idx_silver_payments_payment_ts ON silver_payments_cleaned(payment_ts);
CREATE INDEX idx_silver_complaints_order ON silver_complaints_cleaned(order_id);
CREATE INDEX idx_silver_complaints_customer ON silver_complaints_cleaned(customer_id);
CREATE INDEX idx_silver_complaints_complaint_ts ON silver_complaints_cleaned(complaint_ts);
CREATE INDEX idx_dim_date_date ON gold.dim_date(date);
CREATE INDEX idx_dim_time_hour_minute ON gold.dim_time(hour_24, minute);
