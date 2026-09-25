# 🚀 Apache Airflow & Apache Superset Setup Guide

This document provides a comprehensive, step-by-step guide for setting up, running, and managing **Apache Airflow** and **Apache Superset** when cloning this repository.

---

## 📋 Table of Contents
1. [Prerequisites](#-prerequisites)
2. [Repository Setup & Environment Configuration](#-repository-setup--environment-configuration)
3. [Quick Start via Docker Compose (Recommended)](#-quick-start-via-docker-compose-recommended)
   - [3.1 Start All Services](#31-start-all-services)
   - [3.2 Service URLs & Default Port Reference](#32-service-urls--default-port-reference)
4. [Setting Up & Operating Apache Airflow](#-setting-up--operating-apache-airflow)
   - [4.1 Accessing Airflow UI](#41-accessing-airflow-ui)
   - [4.2 Running the Orchestration DAG](#42-running-the-orchestration-dag)
   - [4.3 Useful Airflow Commands](#43-useful-airflow-commands)
5. [Setting Up & Operating Apache Superset](#-setting-up--operating-apache-superset)
   - [5.1 Accessing Superset UI](#51-accessing-superset-ui)
   - [5.2 Connecting Superset to PostgreSQL Warehouse](#52-connecting-superset-to-postgresql-warehouse)
   - [5.3 Automated Dashboard & Dataset Configuration](#53-automated-dashboard--dataset-configuration)
   - [5.4 Manual Superset Setup (UI Method)](#54-manual-superset-setup-ui-method)
6. [Stopping & Resetting Infrastructure](#-stopping--resetting-infrastructure)
7. [Troubleshooting & FAQs](#-troubleshooting--faqs)

---

## ⚙️ Prerequisites

Before getting started, ensure you have the following installed on your machine:

- **Git**: [Download Git](https://git-scm.com/)
- **Docker & Docker Compose** (v2.0+): [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)
- **Python 3.10+** (Optional, for running local automation scripts): [Download Python](https://www.python.org/)

---

## 🛠 Repository Setup & Environment Configuration

### 1. Clone the Repository
Open your terminal or command prompt and clone the repository:

```bash
git clone https://github.com/your-username/your-repository-name.git
cd your-repository-name
```

### 2. Configure Environment Variables
Copy the provided `.env.example` file to create your local `.env` configuration:

**Linux / macOS / PowerShell:**
```bash
cp .env.example .env
```

**Windows Command Prompt:**
```cmd
copy .env.example .env
```

### 3. Environment Variables Reference (`.env`)
The `.env` file contains configurable parameters for PostgreSQL, Apache Airflow, and Apache Superset:

```ini
# Pipeline Execution Settings
PIPELINE_RUN_MODE=full
LOG_LEVEL=INFO

# Data Lake Paths
DATA_LAKE_BRONZE_PATH=./data_lake/bronze/olist
DATA_LAKE_SILVER_PATH=./data_lake/silver
DATA_LAKE_QUARANTINE_PATH=./data_lake/quarantine

# PostgreSQL Warehouse Connection Settings
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=olist_warehouse
POSTGRES_USER=your_postgres_user
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_JDBC_URL=jdbc:postgresql://postgres:5432/olist_warehouse

# Spark Settings
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=2g
```

---

## 🐳 Quick Start via Docker Compose (Recommended)

The easiest way to run the entire data engineering ecosystem (PostgreSQL, Airflow, and Superset) is using Docker Compose.

### 3.1 Start All Services
From the repository root directory, execute:

```bash
docker compose up -d
```

This starts the following containerized services in detached mode:
1. `olist_postgres`: PostgreSQL 16 Data Warehouse & Airflow Metastore.
2. `olist_airflow_init`: One-time initialization container for database migrations and Airflow admin user creation.
3. `olist_airflow_webserver`: Apache Airflow Web Interface.
4. `olist_airflow_scheduler`: Apache Airflow Task Scheduler.
5. `olist_superset`: Apache Superset BI Web Server & Metastore.

Verify that all containers are running and healthy:

```bash
docker compose ps
```

### 3.2 Service URLs & Default Port Reference

| Service | Host URL | Container Port | Host Port | Default Credentials |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Airflow Webserver** | `http://localhost:8085` | `8080` | `8085` | **User:** `admin`<br>**Password:** `admin` |
| **Apache Superset BI UI** | `http://localhost:8088` | `8088` | `8088` | **User:** `admin`<br>**Password:** `admin` |
| **PostgreSQL Warehouse** | `localhost:5433` | `5432` | `5433` | **DB:** `olist_warehouse`<br>**User:** `olist_admin`<br>**Password:** `olist_password_secure` |

---

## ⚡ Setting Up & Operating Apache Airflow

### 4.1 Accessing Airflow UI
1. Open your browser and navigate to **`http://localhost:8085`**.
2. Log in using the default credentials:
   - **Username**: `admin`
   - **Password**: `admin`

### 4.2 Running the Orchestration DAG
1. In the Airflow DAGs view, locate the DAG named **`olist_data_pipeline`**.
2. Unpause the DAG by clicking the toggle switch on the left side of the DAG row to **ON** (Blue).
3. Trigger the pipeline manually by clicking the **Play (▶)** button on the right side and selecting **Trigger DAG**.
4. Click on **`olist_data_pipeline`** to monitor real-time execution in the **Grid View** or **Graph View**.

#### Pipeline Execution Graph:
```text
start_pipeline ──► create_run_context ──► discover_files ──► validate_file_set
                                                                  │
                                                                  ▼
                                                           create_manifest
                                                                  │
                                                                  ▼
                                                          bronze_to_silver
                                                                  │
                                                                  ▼
                                                      run_data_quality_checks
                                                                  │
                                                                  ▼
                                                           quality_gate
                                                           /          \
                                               [Pass]     /            \  [Fail]
                                                         ▼              ▼
                                                 load_dimensions    write_quarantine
                                                         │
                                                         ▼
                                                     load_facts
                                                         │
                                                         ▼
                                                  run_reconciliation
                                                         │
                                                         ▼
                                              refresh_reporting_views
                                                         │
                                                         ▼
                                                  complete_pipeline
```

### 4.3 Useful Airflow Commands

- **Check Airflow Container Logs:**
  ```bash
  docker compose logs -f airflow-webserver
  docker compose logs -f airflow-scheduler
  ```

- **List All Registered DAGs:**
  ```bash
  docker exec -it olist_airflow_webserver airflow dags list
  ```

- **Test a Specific Airflow Task via CLI:**
  ```bash
  docker exec -it olist_airflow_webserver airflow tasks test olist_data_pipeline run_data_quality_checks 2026-01-01
  ```

---

## 📊 Setting Up & Operating Apache Superset

### 5.1 Accessing Superset UI
1. Open your browser and navigate to **`http://localhost:8088`**.
2. Log in using the default administrator credentials:
   - **Username**: `admin`
   - **Password**: `admin`

### 5.2 Connecting Superset to PostgreSQL Warehouse
When running via Docker Compose, Superset connects to the PostgreSQL container over the Docker internal network:

1. In the Superset top navigation bar, click **Settings** (gear icon) ➔ **Database Connections**.
2. Click the blue **+ Database** button in the top right.
3. Select **PostgreSQL** from the database engine list.
4. Enter the database connection parameters:
   - **Host**: `postgres` (if connecting inside Docker network) or `localhost` (if connecting from host machine).
   - **Port**: `5432` (inside Docker network) or `5433` (from host machine).
   - **Database Name**: `olist_warehouse`
   - **Username**: `olist_admin`
   - **Password**: `olist_password_secure`
   - **Display Name**: `Olist Warehouse`
5. Click **Test Connection**. Once "Connection looks good!" is displayed, click **Connect & Save**.

### 5.3 Automated Dashboard & Dataset Configuration

This repository includes a Python automation script (`src/setup_superset.py`) that exports pre-configured dataset definitions, 11 business KPI metrics, 10 chart specifications, and filter controllers to `reports/superset_dashboard_config.json`.

#### Step 1: Generate Configuration Bundle
Run the generator script locally or inside Docker:

```bash
python src/setup_superset.py
```

#### Step 2: Import Configuration into Superset
- **Option A: Via Superset Web UI**
  1. Go to `http://localhost:8088`.
  2. Click **Settings** ➔ **Import Dashboards**.
  3. Upload the generated file: `reports/superset_dashboard_config.json`.
  4. Enter the database password when prompted (`olist_password_secure`).
  5. Click **Import**.

- **Option B: Via Superset Docker CLI**
  ```bash
  docker exec -it olist_superset superset import-dashboards -p /app/reports/superset_dashboard_config.json
  ```

---

### 5.4 Manual Superset Setup (UI Method)

If you prefer building dashboards manually in the Superset Web UI, follow these steps:

#### Step 1: Register Datasets
1. Go to **Datasets** ➔ **+ Dataset**.
2. Select Database: `Olist Warehouse`, Schema: `reporting`.
3. Select target Analytical Views:
   - `reporting.sales_overview`
   - `reporting.monthly_sales`
   - `reporting.category_performance`
   - `reporting.seller_performance`
   - `reporting.delivery_performance`
   - `reporting.pipeline_health`
   - `reporting.review_performance`
4. Click **Add Dataset and Create Chart**.

#### Step 2: Define Business Metrics
Select dataset `reporting.sales_overview` and add custom SQL metrics:
- **Total Revenue**: `SUM(total_item_price)` (Format: `$,.2f`)
- **Gross Order Value**: `SUM(total_order_value)` (Format: `$,.2f`)
- **Total Orders**: `COUNT(DISTINCT order_id)` (Format: `,.0f`)
- **Average Order Value (AOV)**: `AVG(total_order_value)` (Format: `$,.2f`)

#### Step 3: Create Charts & Assemble Dashboard
1. Go to **Charts** ➔ **+ Chart**.
2. Select dataset `reporting.sales_overview` and chart type (e.g., **Big Number** for KPI, **Time-Series Line** for monthly trends, **Bar Chart** for category performance).
3. Configure metrics, run query, save, and add to **Olist E-Commerce Executive Operational Dashboard**.

---

## 🛑 Stopping & Resetting Infrastructure

### Stop All Running Containers
To temporarily stop services while preserving database data:

```bash
docker compose stop
```

To resume stopped services:

```bash
docker compose start
```

### Shut Down & Clean Container Resources
To stop containers and remove container networks:

```bash
docker compose down
```

### Full Data Reset (Wipe Volumes & Start Fresh)
To perform a complete clean reset (removes PostgreSQL data, Airflow metastore, and Superset state):

```bash
docker compose down -v
docker compose up -d
```

---

## ❓ Troubleshooting & FAQs

### 1. Airflow tasks fail with database connection errors
- **Cause**: PostgreSQL container is still initializing schema when Airflow starts.
- **Fix**: The `docker-compose.yml` uses `depends_on` healthchecks. Run `docker compose restart airflow-webserver airflow-scheduler`.

### 2. Superset cannot connect to PostgreSQL
- **Cause**: Using `localhost` as host inside a Docker container refers to the container itself rather than PostgreSQL.
- **Fix**: Inside Superset UI (running in Docker), use hostname `postgres` and port `5432`. If connecting from a host script outside Docker, use `localhost` and port `5433`.

### 3. Port conflict (Port 8080, 8088, or 5433 already in use)
- **Cause**: Another service or local web server is bound to the target port.
- **Fix**: Update the host port mapping in `docker-compose.yml` (e.g., change `"8085:8080"` to `"8086:8080"` for Airflow, or `"8088:8088"` to `"8089:8088"` for Superset).

### 4. Viewing container logs for debugging
```bash
# View all logs
docker compose logs

# View specific container logs
docker compose logs -f olist_postgres
docker compose logs -f olist_airflow_webserver
docker compose logs -f olist_superset
```

---

*This setup guide is maintained for the Olist E-Commerce Data Engineering Project.*
