-- 03_create_dimensions.sql
-- Create Kimball Star Schema dimension tables in warehouse schema

CREATE TABLE IF NOT EXISTS warehouse.dim_date (
    date_key        INT PRIMARY KEY, -- YYYYMMDD surrogate key
    full_date       DATE NOT NULL UNIQUE,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    month_name      TEXT NOT NULL,
    day             INT NOT NULL,
    day_of_week     INT NOT NULL,
    day_name        TEXT NOT NULL,
    is_weekend      BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_key        SERIAL PRIMARY KEY,
    customer_id         TEXT NOT NULL UNIQUE,
    customer_unique_id  TEXT NOT NULL,
    zip_code_prefix     INT,
    city                TEXT,
    state               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_key         SERIAL PRIMARY KEY,
    product_id          TEXT NOT NULL UNIQUE,
    category_name_pt    TEXT,
    category_name_en    TEXT,
    name_length         INT,
    description_length  INT,
    photos_qty          INT,
    weight_g            NUMERIC(10,2),
    length_cm           NUMERIC(10,2),
    height_cm           NUMERIC(10,2),
    width_cm            NUMERIC(10,2),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_seller (
    seller_key          SERIAL PRIMARY KEY,
    seller_id           TEXT NOT NULL UNIQUE,
    zip_code_prefix     INT,
    city                TEXT,
    state               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_payment_type (
    payment_type_key    SERIAL PRIMARY KEY,
    payment_type_name   TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS warehouse.dim_order_status (
    order_status_key    SERIAL PRIMARY KEY,
    order_status_name   TEXT NOT NULL UNIQUE
);

-- Performance & Reporting Indexes
CREATE INDEX IF NOT EXISTS ind_dim_date_year_month ON warehouse.dim_date(year, month);
CREATE INDEX IF NOT EXISTS ind_dim_customer_state ON warehouse.dim_customer(state);
CREATE INDEX IF NOT EXISTS ind_dim_customer_unique_id ON warehouse.dim_customer(customer_unique_id);
CREATE INDEX IF NOT EXISTS ind_dim_product_category_en ON warehouse.dim_product(category_name_en);
CREATE INDEX IF NOT EXISTS ind_dim_seller_state_city ON warehouse.dim_seller(state, city);


