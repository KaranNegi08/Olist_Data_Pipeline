-- Queries and procedures to verify cross-layer reconciliation metrics and query plan performance

-- Reconciliation 1: Check total revenue between fact_order_item vs fact_order
SELECT 
    'fact_order_item vs fact_order revenue' AS test_name,
    (SELECT SUM(price) FROM warehouse.fact_order_item) AS item_total_price,
    (SELECT SUM(total_item_price) FROM warehouse.fact_order) AS order_total_price,
    ABS(
        (SELECT SUM(price) FROM warehouse.fact_order_item) - 
        (SELECT SUM(total_item_price) FROM warehouse.fact_order)
    ) AS difference;

-- Reconciliation 2: Check total order counts across facts and dimensions
SELECT 
    (SELECT COUNT(*) FROM warehouse.fact_order) AS total_orders,
    (SELECT COUNT(DISTINCT order_id) FROM warehouse.fact_order_item) AS orders_with_items,
    (SELECT COUNT(DISTINCT order_id) FROM warehouse.fact_payment) AS orders_with_payments;

-- ============================================================================
-- QUERY EXPLAIN & EXPLAIN ANALYZE INDEX BENCHMARKING
-- ============================================================================

-- 1. Verify Index Scan on fact_order by date_key (ind_fact_order_date_key)
EXPLAIN ANALYZE
SELECT d.year, d.month_name, SUM(fo.total_order_value) AS monthly_revenue
FROM warehouse.fact_order fo
JOIN warehouse.dim_date d ON fo.date_key = d.date_key
WHERE d.year = 2018
GROUP BY d.year, d.month_name;

-- 2. Verify Index Scan on fact_order_item by product_key (ind_fact_item_product_key)
EXPLAIN ANALYZE
SELECT p.category_name_en, COUNT(foi.order_item_key) AS total_items, SUM(foi.price) AS category_revenue
FROM warehouse.fact_order_item foi
JOIN warehouse.dim_product p ON foi.product_key = p.product_key
GROUP BY p.category_name_en;

-- 3. Verify Index Scan on fact_order by customer_key (ind_fact_order_customer_key)
EXPLAIN ANALYZE
SELECT c.state, COUNT(fo.order_key) AS order_count, SUM(fo.total_order_value) AS total_spend
FROM warehouse.fact_order fo
JOIN warehouse.dim_customer c ON fo.customer_key = c.customer_key
GROUP BY c.state;

-- 4. Verify Index Scan on fact_order_item by seller_key (ind_fact_item_seller_key)
EXPLAIN ANALYZE
SELECT s.seller_id, s.state, SUM(foi.price) AS seller_revenue
FROM warehouse.fact_order_item foi
JOIN warehouse.dim_seller s ON foi.seller_key = s.seller_key
GROUP BY s.seller_id, s.state;


