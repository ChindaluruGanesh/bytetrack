TRUNCATE TABLE gold.dim_date RESTART IDENTITY CASCADE;
TRUNCATE TABLE gold.dim_time RESTART IDENTITY CASCADE;
TRUNCATE TABLE gold.dim_payment_method RESTART IDENTITY CASCADE;

INSERT INTO gold.dim_date (date, year, month, day, is_weekend)
SELECT
    d::date AS date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(DAY FROM d)::INT AS day,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend
FROM generate_series('2023-01-01'::date, '2027-12-31'::date, INTERVAL '1 day') AS d;

INSERT INTO gold.dim_time (hour_24, minute, part_of_day, is_peak_hour)
SELECT
    h AS hour_24,
    m AS minute,
    CASE
        WHEN h BETWEEN 0 AND 5   THEN 'Night'
        WHEN h BETWEEN 6 AND 11  THEN 'Morning'
        WHEN h BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN h BETWEEN 17 AND 21 THEN 'Evening'
        ELSE 'Night'
    END AS part_of_day,
    (h BETWEEN 12 AND 14) OR (h BETWEEN 19 AND 22) AS is_peak_hour
FROM generate_series(0, 23) AS h
CROSS JOIN generate_series(0, 59) AS m;

INSERT INTO gold.dim_payment_method (method_name, is_digital) VALUES
    ('UPI',    TRUE),
    ('Card',   TRUE),
    ('Wallet', TRUE),
    ('Cash',   FALSE);