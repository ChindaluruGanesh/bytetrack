DROP TABLE IF EXISTS gold.dim_order_status CASCADE;
CREATE TABLE gold.dim_order_status (
    status_name VARCHAR(50) PRIMARY KEY,
    status_category VARCHAR(30) NOT NULL,
    is_final_state BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO gold.dim_order_status (status_name, status_category, is_final_state) VALUES
    ('delivered', 'Completed', TRUE),
    ('cancelled', 'Cancelled', TRUE),
    ('pending',   'Active',    FALSE),
    ('refunded',  'Cancelled', TRUE);

DROP TABLE IF EXISTS gold.dim_payment_status CASCADE;
CREATE TABLE gold.dim_payment_status (
    status_name VARCHAR(50) PRIMARY KEY,
    status_category VARCHAR(30) NOT NULL,
    is_final_state BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO gold.dim_payment_status (status_name, status_category, is_final_state) VALUES
    ('success', 'Completed', TRUE),
    ('failed',  'Failed',    TRUE),
    ('pending', 'Active',    FALSE);

DROP TABLE IF EXISTS gold.dim_complaint_category CASCADE;
CREATE TABLE gold.dim_complaint_category (
    category_name VARCHAR(100) PRIMARY KEY,
    category_group VARCHAR(50) NOT NULL,
    severity VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO gold.dim_complaint_category (category_name, category_group) VALUES
    ('Delivery Delay',  'Logistics'),
    ('Wrong Item',      'Food Quality'),
    ('Quality Issue',   'Food Quality'),
    ('Payment Problem', 'Payment');