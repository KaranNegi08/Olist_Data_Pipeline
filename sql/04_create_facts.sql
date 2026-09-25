-- =====================================================================
-- FACT TABLE GRAIN SPECIFICATION:
-- 1. fact_order:      One row per order (Grain: order_id)
-- 2. fact_order_item: One row per order item (Grain: order_id, order_item_id)
-- 3. fact_payment:    One row per payment sequence for an order (Grain: order_id, payment_sequential)
-- 4. fact_delivery:   One row per order delivery (Grain: order_id)
--
-- NOTE: Never join item-level (fact_order_item) and payment-level (fact_payment)
-- rows directly without pre-aggregation. Joining N items with M payments produces
-- an (N x M) Cartesian explosion, corrupting revenue metrics.
-- =====================================================================

-- 1. fact_order: One row per order
CREATE TABLE IF NOT EXISTS warehouse.fact_order (
    order_key                       SERIAL PRIMARY KEY,
    order_id                        TEXT NOT NULL UNIQUE,
    customer_id                     TEXT,
    customer_key                    INT REFERENCES warehouse.dim_customer(customer_key) ON DELETE SET NULL,
    order_status                    TEXT,
    order_status_key                INT REFERENCES warehouse.dim_order_status(order_status_key) ON DELETE SET NULL,
    date_key                        INT NOT NULL REFERENCES warehouse.dim_date(date_key),
    order_purchase_timestamp        TIMESTAMPTZ NOT NULL,
    order_approved_at               TIMESTAMPTZ,
    order_delivered_carrier_date    TIMESTAMPTZ,
    order_delivered_customer_date   TIMESTAMPTZ,
    order_estimated_delivery_date   TIMESTAMPTZ,
    total_item_price                NUMERIC(12,2) DEFAULT 0.00,
    total_freight_value             NUMERIC(12,2) DEFAULT 0.00,
    total_order_value               NUMERIC(12,2) DEFAULT 0.00,
    item_count                      INT DEFAULT 0
);

-- 2. fact_order_item: One row per order item
CREATE TABLE IF NOT EXISTS warehouse.fact_order_item (
    order_item_key                  SERIAL PRIMARY KEY,
    order_id                        TEXT NOT NULL,
    order_item_id                   INT NOT NULL,
    product_key                     INT NOT NULL REFERENCES warehouse.dim_product(product_key),
    seller_key                      INT NOT NULL REFERENCES warehouse.dim_seller(seller_key),
    shipping_limit_date             TIMESTAMPTZ,
    price                           NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    freight_value                   NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    total_item_value                NUMERIC(10,2) GENERATED ALWAYS AS (price + freight_value) STORED,
    CONSTRAINT uq_order_item UNIQUE (order_id, order_item_id)
);

-- 3. fact_payment: One row per payment sequence for an order
CREATE TABLE IF NOT EXISTS warehouse.fact_payment (
    payment_key                     SERIAL PRIMARY KEY,
    order_id                        TEXT NOT NULL,
    payment_sequential              INT NOT NULL,
    payment_type_key                INT NOT NULL REFERENCES warehouse.dim_payment_type(payment_type_key),
    payment_installments            INT NOT NULL DEFAULT 1,
    payment_value                   NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    CONSTRAINT uq_order_payment UNIQUE (order_id, payment_sequential)
);

-- 4. fact_delivery: One row per order delivery
CREATE TABLE IF NOT EXISTS warehouse.fact_delivery (
    delivery_key                    SERIAL PRIMARY KEY,
    order_id                        TEXT NOT NULL UNIQUE,
    customer_id                     TEXT,
    customer_key                    INT REFERENCES warehouse.dim_customer(customer_key) ON DELETE SET NULL,
    date_key                        INT NOT NULL REFERENCES warehouse.dim_date(date_key),
    order_purchase_timestamp        TIMESTAMPTZ NOT NULL,
    order_delivered_customer_date   TIMESTAMPTZ,
    order_estimated_delivery_date   TIMESTAMPTZ,
    delivery_days                   NUMERIC(8,2),
    estimated_delivery_days         NUMERIC(8,2),
    delay_days                      NUMERIC(8,2),
    is_late                         BOOLEAN NOT NULL DEFAULT FALSE
);

-- 5. fact_review: One row per order review score
CREATE TABLE IF NOT EXISTS warehouse.fact_review (
    review_key                      SERIAL PRIMARY KEY,
    review_id                       TEXT NOT NULL,
    order_id                        TEXT NOT NULL,
    review_score                    INT CHECK(review_score BETWEEN 1 AND 5 ),
    review_creation_date            TIMESTAMPTZ,
    review_answer_timestamp         TIMESTAMPTZ
);

-- Indexes for performance tuning & join acceleration
CREATE INDEX IF NOT EXISTS ind_fact_order_date_key ON warehouse.fact_order(date_key);
CREATE INDEX IF NOT EXISTS ind_fact_order_customer_key ON warehouse.fact_order(customer_key);
CREATE INDEX IF NOT EXISTS ind_fact_order_customer_id ON warehouse.fact_order(customer_id);
CREATE INDEX IF NOT EXISTS ind_fact_order_status_key ON warehouse.fact_order(order_status_key);

CREATE INDEX IF NOT EXISTS ind_fact_item_order_id ON warehouse.fact_order_item(order_id);
CREATE INDEX IF NOT EXISTS ind_fact_item_product_key ON warehouse.fact_order_item(product_key);
CREATE INDEX IF NOT EXISTS ind_fact_item_seller_key ON warehouse.fact_order_item(seller_key);

CREATE INDEX IF NOT EXISTS ind_fact_payment_order_id ON warehouse.fact_payment(order_id);
CREATE INDEX IF NOT EXISTS ind_fact_payment_type_key ON warehouse.fact_payment(payment_type_key);

CREATE INDEX IF NOT EXISTS ind_fact_delivery_is_late ON warehouse.fact_delivery(is_late, date_key);
CREATE INDEX IF NOT EXISTS ind_fact_review_score ON warehouse.fact_review(review_score);
CREATE INDEX IF NOT EXISTS ind_fact_review_order_id ON warehouse.fact_review(order_id);



