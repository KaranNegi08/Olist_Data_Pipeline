-- 01_create_schemas.sql
-- Create required PostgreSQL schemas for Data Warehouse pipeline

CREATE SCHEMA IF NOT EXISTS audit;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS reporting;
