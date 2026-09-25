-- 06_load_facts.sql
-- Auxiliary SQL for fact-table loading and validation post-load

-- Verify fact_order row count vs staging
SELECT COUNT(*) AS total_fact_orders FROM warehouse.fact_order;
