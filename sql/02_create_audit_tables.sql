-- 02_create_audit_tables.sql
-- Create audit and watermark tables for pipeline tracking and reconciliation

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    pipeline_run_id     VARCHAR(64) PRIMARY KEY,
    dag_run_id          TEXT,
    load_type           TEXT        NOT NULL DEFAULT 'full' CHECK (load_type IN ('full', 'incremental')),
    started_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at        TIMESTAMPTZ,
    status              TEXT        NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
    source_row_count    BIGINT      DEFAULT 0,
    accepted_row_count  BIGINT      DEFAULT 0,
    rejected_row_count  BIGINT      DEFAULT 0,
    error_message       TEXT
);

CREATE TABLE IF NOT EXISTS audit.file_manifest (
    pipeline_run_id     VARCHAR(64) NOT NULL REFERENCES audit.pipeline_run (pipeline_run_id) ON DELETE CASCADE,
    file_name           TEXT        NOT NULL,
    file_path           TEXT        NOT NULL,
    file_size_bytes     BIGINT      NOT NULL,
    checksum            TEXT        NOT NULL,
    source_row_count    BIGINT      DEFAULT 0,
    header              TEXT,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    status              TEXT        NOT NULL DEFAULT 'DISCOVERED' CHECK (status IN ('DISCOVERED', 'COPIED', 'PROCESSED', 'FAILED')),
    PRIMARY KEY (pipeline_run_id, file_name)
);

CREATE TABLE IF NOT EXISTS audit.data_quality_result (
    result_id           SERIAL      PRIMARY KEY,
    pipeline_run_id     VARCHAR(64) NOT NULL REFERENCES audit.pipeline_run (pipeline_run_id) ON DELETE CASCADE,
    table_name          TEXT        NOT NULL,
    rule_name           TEXT        NOT NULL,
    passed_count        BIGINT      DEFAULT 0,
    failed_count        BIGINT      DEFAULT 0,
    evaluated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit.reconciliation_result (
    reconciliation_id   SERIAL        PRIMARY KEY,
    pipeline_run_id     VARCHAR(64)   NOT NULL REFERENCES audit.pipeline_run (pipeline_run_id) ON DELETE CASCADE,
    source_layer        TEXT          NOT NULL,
    target_layer        TEXT          NOT NULL,
    table_name          TEXT          NOT NULL,
    source_row_count    BIGINT        DEFAULT 0,
    target_row_count    BIGINT        DEFAULT 0,
    row_count_diff      BIGINT        DEFAULT 0,
    source_revenue      NUMERIC(15,2) DEFAULT 0.00,
    target_revenue      NUMERIC(15,2) DEFAULT 0.00,
    revenue_diff        NUMERIC(15,2) DEFAULT 0.00,
    status              TEXT          NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PASSED', 'FAILED')),
    checked_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS audit.load_watermark (
    watermark_id          SERIAL      PRIMARY KEY,
    pipeline_name         TEXT        NOT NULL UNIQUE,
    source_name           TEXT        NOT NULL,
    last_successful_value TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    pipeline_run_id       VARCHAR(64) REFERENCES audit.pipeline_run (pipeline_run_id) ON DELETE SET NULL
);

-- Indexes for performance & reporting
CREATE INDEX IF NOT EXISTS ind_audit_pipeline_run_started ON audit.pipeline_run(started_at DESC);
CREATE INDEX IF NOT EXISTS ind_audit_pipeline_run_status ON audit.pipeline_run(status);
CREATE INDEX IF NOT EXISTS ind_audit_file_manifest_status ON audit.file_manifest(status);
CREATE INDEX IF NOT EXISTS ind_audit_dq_pipeline_run ON audit.data_quality_result(pipeline_run_id);


