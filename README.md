# Olist E-Commerce End-to-End Data Engineering Pipeline

An enterprise-ready, end-to-end Data Lakehouse and Dimensional Data Warehouse pipeline built on the **Olist Brazilian E-Commerce dataset** (~100,000 orders).

---

## 📌 Solution Overview

The **Olist Data Engineering Pipeline** provides a complete batch and incremental ingestion, data quality validation, dimensional modeling, automated audit reconciliation, and visual analytics ecosystem.

### Core Capabilities
* **Pre-Flight Ingestion & Integrity Audit**: Performs raw CSV header validation, non-emptiness checks, and 64-character SHA256 cryptographic checksum calculations for anti-tampering lineage.
* **Medallion Lakehouse Architecture**:
  * **Bronze Layer**: Raw, immutable original CSV storage in `data_lake/bronze/olist/`.
  * **Silver Layer**: PySpark cleaning, timestamp/decimal standardization, business key deduplication, referential integrity verification, and Snappy-compressed Parquet storage in `data_lake/silver/`.
  * **Quarantine Layer**: Automated left-anti join isolation of bad/corrupted records into `data_lake/quarantine/` with rejection metadata (`rejection_reason`, `rejected_at`).
* **Kimball Star Schema Warehouse**: Transforms Silver Parquet data into Star Schema Dimensions (`dim_customer`, `dim_product`, `dim_seller`, `dim_date`) and Fact tables (`fact_order`, `fact_order_item`, `fact_payment`, `fact_delivery`, `fact_review`) loaded into PostgreSQL (`warehouse` schema).
* **Automated Audit & Reconciliation Engine**: 10-point cross-layer audit system verifying row balance equations ($\text{Source} = \text{Accepted} + \text{Rejected} + \text{Deduplicated}$) and exact financial matching using 2-decimal precision (`0.01` tolerance), persisting artifacts to `reports/reconciliation/reconciliation_report.json` and PostgreSQL `audit.reconciliation_result`.
* **Apache Superset BI Integration**: Automated setup exporter generating dataset registrations, 11 business metrics, 10 chart specifications, and cross-dashboard slicers in `reports/superset_dashboard_config.json`.
* **Orchestration**: Fully automated workflow execution supporting both Python CLI orchestrator (`src/run_pipeline.py`) and Apache Airflow DAG (`airflow/dags/olist_pipeline.py`) with quality gates and retries.

---

## 🏗 Architecture

```text
                                 +-------------------------------+
                                 |   Source: Olist CSV Datasets   |
                                 +---------------+---------------+
                                                 |
                                                 v
                                 +---------------+---------------+
                                 |  Bronze Data Lake (Raw CSV)   |
                                 |  data_lake/bronze/olist/      |
                                 +---------------+---------------+
                                                 |
                                                 | (Python File Manifest & Checksums)
                                                 v
                                 +---------------+---------------+
                                 |      PySpark Processing       |
                                 | Schema Enforcement, Validation|
                                 +-------+---------------+-------+
                                         |               |
                       [Invalid Records] |               | [Valid Clean Records]
                                         v               v
                         +---------------+----+     +----+-------------------+
                         |  Quarantine Layer  |     |  Silver Data Lake      |
                         | (Parquet / CSV)    |     | (Clean Parquet Files)  |
                         +--------------------+     +----+-------------------+
                                                         |
                                                         | (PySpark JDBC Batch / Incremental)
                                                         v
                                            +------------+------------+
                                            |   PostgreSQL Warehouse  |
                                            | +---------------------+ |
                                            | | audit schema        | |
                                            | | staging schema      | |
                                            | | warehouse schema    | |
                                            | | reporting schema    | |
                                            | +---------------------+ |
                                            +------------+------------+
                                                         |
                                                         v
                                            +------------+------------+
                                            |     Apache Superset     |
                                            |  Analytical Dashboards  |
                                            +-------------------------+

====================================================================================================
               Controlled & Orchestrated by Apache Airflow DAG (olist_data_pipeline)
====================================================================================================
```

---

## 🛠 Technology Versions

| Tool / Technology | Version | Purpose |
| :--- | :--- | :--- |
| **Python** | `3.11+ / 3.14` | Pre-flight manifest, SHA256 checksums, config parser, logging, orchestrator. |
| **PySpark** | `3.5.3 / 3.5.8` | Distributed ETL processing, schema validation, quarantine routing, Parquet I/O. |
| **PostgreSQL** | `16` | Relational Data Warehouse, Kimball Star Schema, Audit logs, Watermarks, Reporting views. |
| **Apache Airflow** | `2.x / 3.x` | Orchestration DAG, task retries, quality gate evaluation, execution graph. |
| **Apache Parquet** | `--` | Snappy-compressed columnar storage format for Silver & Quarantine layers. |
| **Apache Superset** | `4.x` | Enterprise visual analytics, BI dashboards, dataset & metric management. |
| **Docker Compose** | `v2` | Containerized infrastructure management for PostgreSQL, Airflow, and Superset. |
| **Pytest** | `9.x` | Automated unit and integration testing suite. |

---

## ⚡ Setup Steps

> 💡 **Dedicated Airflow & Superset Guide**: For detailed step-by-step instructions on running Apache Airflow and Apache Superset, check out [`SETUP.md`](file:///c:/Users/Asus/Desktop/DataEngineering_Assignment/SETUP.md).

### Step 1: Clone Repository & Create Environment
```powershell
# Clone repository
git clone https://github.com/your-username/your-repository-name.git
cd your-repository-name

# Create Python virtual environment
python -m venv .venv

# Activate virtual environment (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# Activate virtual environment (Linux / macOS)
# source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Configure Environment Variables
Copy the sample environment template file to create your active `.env`:
```powershell
cp .env.example .env
```

### Step 3: Launch Container Infrastructure (PostgreSQL & Superset)
```powershell
docker compose up -d postgres superset
```

---

## 🔑 Environment Variables

The pipeline reads environment configuration from the `.env` file (placeholders shown below for privacy):

```ini
# PostgreSQL Warehouse Connection Settings
POSTGRES_HOST=your_postgres_host
POSTGRES_PORT=5432
POSTGRES_DB=your_database_name
POSTGRES_USER=your_postgres_username
POSTGRES_PASSWORD=your_secure_password_here
POSTGRES_JDBC_URL=jdbc:postgresql://your_postgres_host:5432/your_database_name

# Apache Airflow Configuration
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# Apache Superset Configuration
SUPERSET_SECRET_KEY=your_secure_random_superset_secret_key_here
SUPERSET_PORT=8088
```

---

## 🔄 Full-Load Command

To execute a **complete full refresh load** across all raw Bronze datasets, process Silver Parquet files, rebuild PostgreSQL Warehouse tables, execute reconciliation checks, and run the test suite:

```powershell
python src/run_pipeline.py --load-type full
```

### Direct PySpark Job Execution (Full Load)
```powershell
# Step 1: PySpark Bronze to Silver
python spark_jobs/bronze_to_silver.py --load-type full

# Step 2: PySpark Silver to Warehouse
python spark_jobs/silver_to_warehouse.py --load-type full
```

---

## 📈 Incremental-Load Command

To execute an **incremental batch load** (processing orders on or after `batch_date_split` / last watermark timestamp):

```powershell
python src/run_pipeline.py --load-type incremental
```

### Direct PySpark Job Execution (Incremental Load)
```powershell
# Step 1: PySpark Bronze to Silver Incremental
python spark_jobs/bronze_to_silver.py --load-type incremental

# Step 2: PySpark Silver to Warehouse Incremental
python spark_jobs/silver_to_warehouse.py --load-type incremental
```

### Airflow DAG Incremental Trigger
In the Airflow UI, trigger `olist_data_pipeline` with configuration JSON:
```json
{
  "load_type": "incremental"
}
```

---

## 💨 How to Open Airflow

1. **Start Airflow Services** (if running via Docker Compose):
   ```powershell
   docker compose up -d airflow-webserver airflow-scheduler
   ```
2. **Access Web Interface**:
   Open browser and navigate to: [http://localhost:8080](http://localhost:8080)
3. **Login Credentials**:
   * **Username**: `your_airflow_username`
   * **Password**: `your_airflow_password`
4. **Trigger DAG**: Locate `olist_data_pipeline` in the DAG list, unpause it, and click **Trigger DAG**.

---

## 📊 How to Open Superset

1. **Start Superset Container**:
   ```powershell
   docker compose up -d superset
   ```
2. **Access Web Interface**:
   Open browser and navigate to: [http://localhost:8088](http://localhost:8088)
3. **Login Credentials**:
   * **Username**: `your_superset_username`
   * **Password**: `your_superset_password`
4. **Import Dashboard Configuration Bundle**:
   * **Via UI**: Navigate to **Dashboards** → Click **Import Dashboard** (top right icon) → Upload `reports/superset_dashboard_config.json`.
   * **Via Docker CLI**:
     ```powershell
     docker exec -it olist_superset superset import-dashboards -p /app/reports/superset_dashboard_config.json
     ```

---

## ✅ How to Verify the Results

### 1. View Reconciliation Report JSON
Inspect the output report generated by the Reconciliation Engine:
```powershell
Get-Content reports/reconciliation/reconciliation_report.json | ConvertFrom-Json
```

### 2. Query PostgreSQL Audit Tables
Connect to PostgreSQL and inspect audit execution logs:
```sql
-- View pipeline execution runs & status
SELECT * FROM audit.pipeline_run ORDER BY started_at DESC;

-- View file manifest checksum records
SELECT * FROM audit.file_manifest;

-- View cross-layer reconciliation audit metrics
SELECT * FROM audit.reconciliation_result ORDER BY checked_at DESC;

-- View last watermark timestamp for incremental loading
SELECT * FROM audit.load_watermark;
```

### 3. Run Pytest Automated Test Suite
Execute the full test suite covering data quality rules, referential integrity, and pipeline logging:
```powershell
python -m pytest tests/
```

---

## 🔁 How to Rerun a Failed Batch

If a pipeline batch fails during execution:

1. **Diagnose Root Cause**: Inspect logs in `logs/pipeline.log` or query PostgreSQL audit log:
   ```sql
   SELECT pipeline_run_id, status, error_message, started_at 
   FROM audit.pipeline_run 
   WHERE status = 'FAILED' 
   ORDER BY started_at DESC;
   ```
2. **Resolve Underlying Issue**: Correct any missing configuration, environment variables, or database connectivity issues.
3. **Rerun Pipeline**:
   * **Full Batch Re-run**:
     ```powershell
     python src/run_pipeline.py --load-type full
     ```
   * **Airflow UI Re-run**: In Airflow UI DAG view, select the failed task node and click **Clear** to re-execute down-stream tasks.
4. **Idempotency Guarantee**: PySpark writes use `mode("overwrite")` and PostgreSQL dimensional loads use idempotent `ON CONFLICT DO UPDATE` constraints, ensuring re-executing a batch is safe and will never produce duplicate records.

---

## ⚠️ Known Limitations

1. **Static Historical Dataset**: The Olist dataset is a fixed Kaggle historical archive (2016–2018). Incremental mode uses `batch_date_split` (`2018-06-01`) as the default cutoff boundary to demonstrate incremental processing behavior.
2. **Standalone PySpark Mode**: Local execution runs PySpark in `local[*]` standalone mode. For multi-TB production scaling, PySpark jobs should be deployed to a distributed cluster (e.g. AWS EMR, Databricks, or EKS).
3. **Static Raw Schema Definition**: Schema definitions in `config/table_schemas.yml` are fixed to the 9 Olist CSV structures. Source schema evolution (adding/removing CSV columns) requires updating the YAML configuration file.
