-- Create analytical views in reporting schema for Apache Superset dashboards

-- 1. Daily Sales Performance
CREATE OR REPLACE VIEW reporting.daily_sales AS
SELECT 
    d.full_date,
    d.year,
    d.month,
    d.day_name,
    COUNT(fo.order_key) AS total_orders,
    SUM(fo.total_item_price) AS total_revenue,
    SUM(fo.total_freight_value) AS total_freight,
    SUM(fo.total_order_value) AS gross_order_value,
    AVG(fo.total_order_value) AS avg_order_value
FROM warehouse.fact_order fo
JOIN warehouse.dim_date d ON fo.date_key = d.date_key
GROUP BY d.full_date, d.year, d.month, d.day_name;

-- 2. Monthly Sales & Growth Metrics
CREATE OR REPLACE VIEW reporting.monthly_sales AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(fo.order_key) AS monthly_order_count,
    SUM(fo.total_item_price) AS monthly_item_revenue,
    SUM(fo.total_freight_value) AS monthly_freight_amount,
    SUM(fo.total_order_value) AS monthly_gross_revenue
FROM warehouse.fact_order fo
JOIN warehouse.dim_date d ON fo.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name;

-- 3. Top Product Categories by Revenue
CREATE OR REPLACE VIEW reporting.category_performance AS
SELECT 
    p.category_name_en AS product_category,
    COUNT(foi.order_item_key) AS items_sold,
    SUM(foi.price) AS total_category_revenue,
    AVG(foi.price) AS avg_item_price
FROM warehouse.fact_order_item foi
JOIN warehouse.dim_product p ON foi.product_key = p.product_key
GROUP BY p.category_name_en;

-- 4. Seller Revenue Performance & Ranking
CREATE OR REPLACE VIEW reporting.seller_performance AS
SELECT 
    s.seller_id,
    s.city AS seller_city,
    s.state AS seller_state,
    COUNT(DISTINCT foi.order_id) AS total_orders_fulfilled,
    SUM(foi.price) AS seller_total_revenue,
    DENSE_RANK() OVER (PARTITION BY s.state ORDER BY SUM(foi.price) DESC) AS state_revenue_rank
FROM warehouse.fact_order_item foi
JOIN warehouse.dim_seller s ON foi.seller_key = s.seller_key
GROUP BY s.seller_id, s.city, s.state;

-- 5. Delivery Performance & SLA Metrics by Customer State
CREATE OR REPLACE VIEW reporting.delivery_performance AS
SELECT 
    c.state AS customer_state,
    COUNT(fd.delivery_key) AS total_deliveries,
    AVG(fd.delivery_days) AS avg_delivery_days,
    AVG(fd.estimated_delivery_days) AS avg_estimated_days,
    SUM(CASE WHEN fd.is_late THEN 1 ELSE 0 END) AS late_deliveries,
    ROUND((SUM(CASE WHEN fd.is_late THEN 1 ELSE 0 END)::NUMERIC / COUNT(fd.delivery_key)) * 100, 2) AS late_delivery_percentage
FROM warehouse.fact_delivery fd
JOIN warehouse.dim_customer c ON fd.customer_key = c.customer_key
GROUP BY c.state;

-- 6. Customer Order Summary & Repeat Rate
CREATE OR REPLACE VIEW reporting.customer_summary AS
SELECT 
    c.customer_unique_id,
    c.state AS customer_state,
    COUNT(fo.order_key) AS order_count,
    SUM(fo.total_order_value) AS lifetime_spend,
    CASE WHEN COUNT(fo.order_key) > 1 THEN TRUE ELSE FALSE END AS is_repeat_customer
FROM warehouse.fact_order fo
JOIN warehouse.dim_customer c ON fo.customer_key = c.customer_key
GROUP BY c.customer_unique_id, c.state;

-- 7. Pipeline Execution & Data Quality Health
CREATE OR REPLACE VIEW reporting.pipeline_health AS
SELECT 
    pr.pipeline_run_id,
    pr.dag_run_id,
    pr.load_type,
    pr.started_at,
    pr.completed_at,
    pr.status,
    pr.source_row_count,
    pr.accepted_row_count,
    pr.rejected_row_count,
    ROUND((pr.rejected_row_count::NUMERIC / NULLIF(pr.source_row_count, 0)) * 100, 2) AS rejection_rate_percentage
FROM audit.pipeline_run pr;

-- 8. Customer Order Review Performance & Score Distribution
CREATE OR REPLACE VIEW reporting.review_performance AS
SELECT 
    fr.review_score,
    COUNT(fr.review_id) AS total_reviews,
    ROUND(AVG(fr.review_score), 2) AS avg_review_score
FROM warehouse.fact_review fr
GROUP BY fr.review_score;


-- 9. Comprehensive Order Sales Overview (Safe Non-Duplicated Grain for Superset Filters)
CREATE OR REPLACE VIEW reporting.sales_overview AS
SELECT 
    fo.order_id,
    fo.order_purchase_timestamp::DATE AS purchase_date,
    fo.order_purchase_timestamp,
    fo.order_status,
    c.customer_unique_id,
    c.state AS customer_state,
    c.city AS customer_city,
    fo.total_item_price AS total_revenue,
    fo.total_freight_value AS total_freight,
    fo.total_order_value AS gross_order_value,
    fo.item_count
FROM warehouse.fact_order fo
LEFT JOIN warehouse.dim_customer c ON fo.customer_key = c.customer_key;

