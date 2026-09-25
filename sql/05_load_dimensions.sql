-- 05_load_dimensions.sql
-- Seed fixed dimension data like dim_date, dim_payment_type, and dim_order_status

-- Populate dim_date from 2016-01-01 to 2018-12-31
INSERT INTO warehouse.dim_date (
    date_key, full_date, year, quarter, month, month_name, day, day_of_week, day_name, is_weekend
)
SELECT 
    TO_CHAR(datum, 'YYYYMMDD')::INT AS date_key,
    datum AS full_date,
    EXTRACT(YEAR FROM datum)::INT AS year,
    EXTRACT(QUARTER FROM datum)::INT AS quarter,
    EXTRACT(MONTH FROM datum)::INT AS month,
    TO_CHAR(datum, 'Month') AS month_name,
    EXTRACT(DAY FROM datum)::INT AS day,
    EXTRACT(ISODOW FROM datum)::INT AS day_of_week,
    TO_CHAR(datum, 'Day') AS day_name,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series(
    '2015-01-01'::DATE,
    '2030-12-31'::DATE,
    '1 day'::INTERVAL
) AS datum
ON CONFLICT (date_key) DO NOTHING;

-- Seed dim_order_status defaults
INSERT INTO warehouse.dim_order_status (order_status_name) VALUES
('delivered'), ('shipped'), ('canceled'), ('invoiced'), ('processing'), ('created'), ('approved'), ('unavailable')
ON CONFLICT (order_status_name) DO NOTHING;

-- Seed dim_payment_type defaults
INSERT INTO warehouse.dim_payment_type (payment_type_name) VALUES
('credit_card'), ('boleto'), ('voucher'), ('debit_card'), ('not_defined')
ON CONFLICT (payment_type_name) DO NOTHING;
