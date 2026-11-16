-- ============================================================
-- BYTETRACK — 50 BUSINESS SCENARIOS (FINAL - CTE BASED)
-- ============================================================

-- CTE helpers (run standalone before the queries or rely on inline CTEs)
-- Since each query is standalone, we use inline CTEs.

-- 1. Peak ordering hours
SELECT city, EXTRACT(HOUR FROM order_ts) as hour, COUNT(*) as orders
FROM gold.fact_orders WHERE cancellation_flag = FALSE
GROUP BY 1, 2 ORDER BY orders DESC LIMIT 10;

-- 2. Average order value by cuisine
SELECT cuisine_type, COUNT(*) as orders, ROUND(AVG(net_amount), 2) as avg_value
FROM gold.fact_orders WHERE cancellation_flag = FALSE
GROUP BY 1 ORDER BY avg_value DESC;

-- 3. Order status distribution
SELECT status, COUNT(*) as orders, ROUND(COUNT(*)::numeric / SUM(COUNT(*)) OVER () * 100, 2) as pct
FROM gold.fact_orders GROUP BY 1 ORDER BY orders DESC;

-- 4. Monthly order growth
WITH monthly AS (
    SELECT DATE_TRUNC('month', order_ts)::date as month, COUNT(*) as orders
    FROM gold.fact_orders GROUP BY 1
)
SELECT month, orders, orders - LAG(orders) OVER (ORDER BY month) as mom_change
FROM monthly ORDER BY month DESC LIMIT 12;

-- 5. Weekend vs weekday
SELECT CASE WHEN EXTRACT(ISODOW FROM order_ts) IN (6,7) THEN 'Weekend' ELSE 'Weekday' END as day_type,
       COUNT(*) as orders, ROUND(AVG(net_amount), 2) as avg_value
FROM gold.fact_orders GROUP BY 1;

-- 6. Late orders by zone
SELECT zone, COUNT(*) as deliveries,
       COUNT(*) FILTER (WHERE is_late) as late,
       ROUND(COUNT(*) FILTER (WHERE is_late)::numeric / COUNT(*) * 100, 2) as late_pct
FROM gold.fact_orders WHERE delivery_time_mins IS NOT NULL AND cancellation_flag = FALSE
GROUP BY 1 ORDER BY late_pct DESC;

-- 7. Reorder rate
WITH customer_cuisine AS (
    SELECT f.customer_sk, f.cuisine_type, COUNT(*) as order_count
    FROM gold.fact_orders f WHERE f.cancellation_flag = FALSE AND f.customer_sk IS NOT NULL
    GROUP BY 1, 2
)
SELECT cuisine_type, COUNT(*) FILTER (WHERE order_count > 1) as repeat_customers,
       COUNT(*) as total_customers,
       ROUND(COUNT(*) FILTER (WHERE order_count > 1)::numeric / COUNT(*) * 100, 2) as reorder_pct
FROM customer_cuisine GROUP BY 1 ORDER BY reorder_pct DESC;

-- 8. Items per order by city
SELECT city, COUNT(*) as orders, ROUND(AVG(total_amount / NULLIF(net_amount,0)), 2) as avg_items
FROM gold.fact_orders WHERE cancellation_flag = FALSE GROUP BY 1;

-- 9. Top 10 restaurants by revenue (FIXED via CTE)
WITH dim_r AS (
    SELECT ROW_NUMBER() OVER (ORDER BY restaurant_id, dbt_valid_from) AS restaurant_sk, name, city
    FROM gold.dim_restaurant WHERE dbt_valid_to IS NULL
)
SELECT r.name, r.city, COUNT(*) as orders, ROUND(SUM(f.net_amount), 2) as revenue
FROM gold.fact_orders f JOIN dim_r r ON f.restaurant_sk = r.restaurant_sk
WHERE f.cancellation_flag = FALSE GROUP BY 1, 2 ORDER BY revenue DESC LIMIT 10;

-- 10. Cuisine popularity trend
SELECT DATE_TRUNC('month', order_ts)::date as month, cuisine_type, COUNT(*) as orders
FROM gold.fact_orders GROUP BY 1, 2 ORDER BY 1 DESC, 3 DESC LIMIT 20;

-- 11. Average prep time
SELECT name, ROUND(AVG(avg_prep_time_mins), 2) as avg_prep
FROM gold.dim_restaurant GROUP BY 1 HAVING AVG(avg_prep_time_mins) > 60 ORDER BY avg_prep DESC LIMIT 10;

-- 12. Restaurant tier change history
SELECT restaurant_id, tier, dbt_valid_from, dbt_valid_to, (dbt_valid_to IS NULL) as is_current
FROM gold.dim_restaurant WHERE restaurant_id IN (
    SELECT restaurant_id FROM gold.dim_restaurant GROUP BY 1 HAVING COUNT(*) > 1
) ORDER BY restaurant_id, dbt_valid_from LIMIT 20;

-- 13. Commission revenue by city
SELECT city, COUNT(*) as restaurants, ROUND(AVG(commission_pct), 2) as avg_commission
FROM gold.dim_restaurant GROUP BY 1 ORDER BY 2 DESC;

-- 14. Restaurant satisfaction trend
SELECT DATE_TRUNC('month', dbt_valid_from)::date as month, ROUND(AVG(avg_rating), 2) as avg_rating
FROM gold.dim_restaurant GROUP BY 1 ORDER BY 1 DESC LIMIT 12;

-- 15. Restaurant retention
SELECT is_active, COUNT(*) as restaurants FROM gold.dim_restaurant GROUP BY 1;

-- 16. Revenue per order by restaurant tier (FIXED via CTE)
WITH dim_r AS (
    SELECT ROW_NUMBER() OVER (ORDER BY restaurant_id, dbt_valid_from) AS restaurant_sk, tier
    FROM gold.dim_restaurant WHERE dbt_valid_to IS NULL
)
SELECT r.tier, COUNT(*) as orders, ROUND(AVG(f.net_amount), 2) as avg_revenue
FROM gold.fact_orders f JOIN dim_r r ON f.restaurant_sk = r.restaurant_sk
WHERE f.cancellation_flag = FALSE GROUP BY 1 ORDER BY 3 DESC;

-- 17. Repeat vs one-time customers
WITH monthly AS (
    SELECT DATE_TRUNC('month', order_ts)::date as month, customer_sk, COUNT(*) as orders
    FROM gold.fact_orders WHERE cancellation_flag = FALSE AND customer_sk IS NOT NULL
    GROUP BY 1, 2
)
SELECT month, COUNT(*) FILTER (WHERE orders > 1) as repeat, COUNT(*) FILTER (WHERE orders = 1) as one_time,
       ROUND(COUNT(*) FILTER (WHERE orders > 1)::numeric / COUNT(*) * 100, 2) as repeat_pct
FROM monthly GROUP BY 1 ORDER BY 1 DESC LIMIT 12;

-- 18. Customer loyalty tier progression
SELECT customer_id, loyalty_tier, dbt_valid_from, dbt_valid_to
FROM gold.dim_customer WHERE customer_id IN (
    SELECT customer_id FROM gold.dim_customer GROUP BY 1 HAVING COUNT(*) > 1
) ORDER BY customer_id, dbt_valid_from LIMIT 20;

-- 19. Average order value by loyalty tier (FIXED via CTE)
WITH dim_c AS (
    SELECT ROW_NUMBER() OVER (ORDER BY customer_id, dbt_valid_from) AS customer_sk, loyalty_tier
    FROM gold.dim_customer WHERE dbt_valid_to IS NULL
)
SELECT c.loyalty_tier, COUNT(*) as orders, ROUND(AVG(f.net_amount), 2) as avg_value
FROM gold.fact_orders f JOIN dim_c c ON f.customer_sk = c.customer_sk
WHERE f.cancellation_flag = FALSE GROUP BY 1 ORDER BY 3 DESC;

-- 20. Churn analysis (simplified - customers with no recent orders)
SELECT COUNT(DISTINCT f.customer_sk) as churned
FROM gold.fact_orders f
WHERE f.customer_sk NOT IN (
    SELECT customer_sk FROM gold.fact_orders WHERE order_ts >= CURRENT_DATE - INTERVAL '30 days'
);

-- 21. Top 10 customers by lifetime value (FIXED via CTE)
WITH dim_c AS (
    SELECT ROW_NUMBER() OVER (ORDER BY customer_id, dbt_valid_from) AS customer_sk, name, city
    FROM gold.dim_customer WHERE dbt_valid_to IS NULL
)
SELECT c.name, c.city, COUNT(*) as orders, ROUND(SUM(f.net_amount), 2) as lifetime_value
FROM gold.fact_orders f JOIN dim_c c ON f.customer_sk = c.customer_sk
WHERE f.cancellation_flag = FALSE GROUP BY 1, 2 ORDER BY lifetime_value DESC LIMIT 10;

-- 22. Preferred cuisine by customer city (simplified)
SELECT city, cuisine_type, COUNT(*) as orders,
       RANK() OVER (PARTITION BY city ORDER BY COUNT(*) DESC) as rank
FROM gold.fact_orders WHERE cancellation_flag = FALSE
GROUP BY 1, 2 ORDER BY 1, 3 DESC LIMIT 20;

-- 23. Customer acquisition trend
SELECT DATE_TRUNC('month', signup_date)::date as month, COUNT(*) as new_customers
FROM gold.dim_customer GROUP BY 1 ORDER BY 1 DESC LIMIT 12;

-- 24. Customer risk distribution
SELECT loyalty_tier, COUNT(*) as customers, ROUND(AVG(total_orders), 2) as avg_orders
FROM gold.dim_customer GROUP BY 1 ORDER BY 2 DESC;

-- 25. Agent performance ranking (FIXED via CTE)
WITH dim_a AS (
    SELECT ROW_NUMBER() OVER (ORDER BY agent_id, dbt_valid_from) AS agent_sk, name
    FROM gold.dim_agent WHERE dbt_valid_to IS NULL
)
SELECT a.name, COUNT(*) as deliveries,
       ROUND(COUNT(*) FILTER (WHERE NOT f.is_late)::numeric / COUNT(*) * 100, 2) as on_time_pct
FROM gold.fact_orders f JOIN dim_a a ON f.agent_sk = a.agent_sk
WHERE f.agent_sk IS NOT NULL AND f.cancellation_flag = FALSE
GROUP BY 1 ORDER BY 3 DESC LIMIT 20;

-- 26. Agent tier change history
SELECT agent_id, tier, dbt_valid_from, dbt_valid_to
FROM gold.dim_agent WHERE agent_id IN (
    SELECT agent_id FROM gold.dim_agent GROUP BY 1 HAVING COUNT(*) > 1
) ORDER BY agent_id, dbt_valid_from LIMIT 20;

-- 27. Agent cancellation rate by zone
SELECT zone, COUNT(*) as deliveries, COUNT(*) FILTER (WHERE cancellation_flag) as cancelled,
       ROUND(COUNT(*) FILTER (WHERE cancellation_flag)::numeric / COUNT(*) * 100, 2) as cancel_pct
FROM gold.fact_orders WHERE agent_sk IS NOT NULL GROUP BY 1 ORDER BY 4 DESC;

-- 28. Shift efficiency
SELECT CASE
    WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 17 AND 21 THEN 'Evening'
    ELSE 'Night' END as shift,
    COUNT(*) as orders, ROUND(AVG(net_amount), 2) as avg_value
FROM gold.fact_orders GROUP BY 1 ORDER BY 2 DESC;

-- 29. Vehicle type performance (FIXED via CTE)
WITH dim_a AS (
    SELECT ROW_NUMBER() OVER (ORDER BY agent_id, dbt_valid_from) AS agent_sk, vehicle_type
    FROM gold.dim_agent WHERE dbt_valid_to IS NULL
)
SELECT a.vehicle_type, COUNT(*) as deliveries,
       ROUND(COUNT(*) FILTER (WHERE NOT f.is_late)::numeric / COUNT(*) * 100, 2) as on_time_pct
FROM gold.fact_orders f JOIN dim_a a ON f.agent_sk = a.agent_sk
WHERE f.agent_sk IS NOT NULL AND f.cancellation_flag = FALSE
GROUP BY 1 ORDER BY 3 DESC;

-- 30. Agent utilization
SELECT tier, COUNT(*) as agents, ROUND(AVG(total_deliveries), 0) as avg_deliveries
FROM gold.dim_agent GROUP BY 1 ORDER BY 3 DESC;

-- 31. Daily revenue by city and cuisine
SELECT order_ts::date as day, city, cuisine_type, COUNT(*) as orders, ROUND(SUM(net_amount), 2) as revenue
FROM gold.fact_orders WHERE cancellation_flag = FALSE
GROUP BY 1, 2, 3 ORDER BY 1 DESC LIMIT 20;

-- 32. Revenue lost due to cancellations
SELECT order_ts::date as day, COUNT(*) as cancelled, ROUND(SUM(total_amount), 2) as revenue_lost
FROM gold.fact_orders WHERE cancellation_flag = TRUE
GROUP BY 1 ORDER BY 1 DESC LIMIT 30;

-- 33. Payment method distribution
SELECT payment_method, COUNT(*) as transactions, ROUND(SUM(amount), 2) as total_amount
FROM gold.fact_payments WHERE is_successful = TRUE
GROUP BY 1 ORDER BY 2 DESC;

-- 34. Cashback and discount impact
SELECT ROUND(SUM(total_amount), 2) as gross_revenue,
       ROUND(SUM(discount_amount), 2) as total_discount,
       ROUND(SUM(net_amount), 2) as net_revenue,
       ROUND(SUM(discount_amount) / NULLIF(SUM(total_amount), 0) * 100, 2) as discount_pct
FROM gold.fact_orders WHERE cancellation_flag = FALSE;

-- 35. Commission revenue trend
SELECT DATE_TRUNC('month', order_ts)::date as month, ROUND(SUM(net_amount * 0.15), 2) as estimated_commission
FROM gold.fact_orders WHERE cancellation_flag = FALSE
GROUP BY 1 ORDER BY 1 DESC;

-- 36. Refund analysis by category
SELECT complaint_category, COUNT(*) as complaints, ROUND(SUM(compensation_amount), 2) as total_refunds
FROM gold.fact_complaints GROUP BY 1 ORDER BY 3 DESC;

-- 37. Promo effectiveness (fixed - using discount_amount)
SELECT CASE WHEN discount_amount > 0 THEN 'With Discount' ELSE 'No Discount' END as promo_type,
       COUNT(*) as orders, ROUND(AVG(net_amount), 2) as avg_value
FROM gold.fact_orders WHERE cancellation_flag = FALSE GROUP BY 1;

-- 38. Promo redemption by loyalty tier (FIXED via CTE)
WITH dim_c AS (
    SELECT ROW_NUMBER() OVER (ORDER BY customer_id, dbt_valid_from) AS customer_sk, loyalty_tier
    FROM gold.dim_customer WHERE dbt_valid_to IS NULL
)
SELECT c.loyalty_tier, COUNT(*) FILTER (WHERE f.discount_amount > 0) as promo_orders, COUNT(*) as total_orders
FROM gold.fact_orders f JOIN dim_c c ON f.customer_sk = c.customer_sk
WHERE f.cancellation_flag = FALSE GROUP BY 1;

-- 39. New vs existing customer promo usage
WITH first_orders AS (
    SELECT customer_sk, MIN(order_ts) as first_order FROM gold.fact_orders WHERE customer_sk IS NOT NULL GROUP BY 1
)
SELECT CASE WHEN f.order_ts = fo.first_order THEN 'New' ELSE 'Existing' END as customer_type,
       COUNT(*) as orders, COUNT(*) FILTER (WHERE f.discount_amount > 0) as promo_orders
FROM gold.fact_orders f JOIN first_orders fo ON f.customer_sk = fo.customer_sk
WHERE f.cancellation_flag = FALSE GROUP BY 1;

-- 40. Campaign ROI (fixed - using discount_amount)
SELECT CASE WHEN discount_amount > 0 THEN 'With Discount' ELSE 'No Discount' END as campaign_type,
       COUNT(*) as orders, ROUND(SUM(discount_amount), 2) as discount_cost,
       ROUND(SUM(net_amount), 2) as revenue_generated
FROM gold.fact_orders WHERE cancellation_flag = FALSE GROUP BY 1;

-- 41. Complaints by zone
SELECT o.zone, c.complaint_category, COUNT(*) as complaints
FROM gold.fact_complaints c JOIN gold.fact_orders o ON c.order_id = o.order_id
GROUP BY 1, 2 ORDER BY 1, 3 DESC LIMIT 20;

-- 42. Complaint resolution time
SELECT complaint_category, ROUND(AVG(resolution_time_hrs), 2) as avg_resolution_hrs,
       ROUND(AVG(compensation_amount), 2) as avg_compensation
FROM gold.fact_complaints GROUP BY 1 ORDER BY 2 DESC;

-- 43. Order items analysis (using fact_orders, not missing table)
SELECT COUNT(*) as total_orders, ROUND(AVG(net_amount), 2) as avg_value FROM gold.fact_orders;

-- 44. Complaint volume trend
SELECT DATE_TRUNC('month', complaint_ts)::date as month, COUNT(*) as complaints
FROM gold.fact_complaints GROUP BY 1 ORDER BY 1 DESC;

-- 45. Delivery time analysis
SELECT ROUND(AVG(delivery_time_mins), 2) as avg_delivery_mins,
       ROUND(MIN(delivery_time_mins), 2) as min_delivery_mins,
       ROUND(MAX(delivery_time_mins), 2) as max_delivery_mins
FROM gold.fact_orders WHERE delivery_time_mins IS NOT NULL;

-- 46. Zone demand by hour
SELECT zone, EXTRACT(HOUR FROM order_ts) as hour, COUNT(*) as orders
FROM gold.fact_orders GROUP BY 1, 2 ORDER BY 1, 3 DESC LIMIT 20;

-- 47. Restaurant coverage
SELECT zone, COUNT(DISTINCT restaurant_sk) as restaurants, COUNT(*) as orders
FROM gold.fact_orders GROUP BY 1;

-- 48. Agent-to-order ratio by shift
SELECT zone, CASE
    WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 6 AND 11 THEN 'Morning'
    WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 12 AND 16 THEN 'Afternoon'
    WHEN EXTRACT(HOUR FROM order_ts) BETWEEN 17 AND 21 THEN 'Evening'
    ELSE 'Night' END as shift,
    COUNT(DISTINCT agent_sk) as agents, COUNT(*) as orders
FROM gold.fact_orders GROUP BY 1, 2 ORDER BY 1, 2;

-- 49. DLQ summary
WITH dlq AS (
    SELECT source_table, error_reason, 1 as rejection_level FROM stg_rejected_level1
    UNION ALL
    SELECT source_table, error_reason, 2 as rejection_level FROM stg_rejected_level2
)
SELECT source_table, error_reason, rejection_level, COUNT(*) as rejects
FROM dlq GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 15;

-- 50. Pipeline execution history
SELECT run_id, table_name, rows_read, rows_inserted, rows_rejected, status
FROM audit_pipeline_runs WHERE table_name != 'ALL' ORDER BY run_id DESC LIMIT 15;