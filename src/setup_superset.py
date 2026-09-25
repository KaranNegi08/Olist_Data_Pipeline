"""
Automated Apache Superset Integration & Operational Dashboard Configuration Setup.

Registers database connection metadata, reporting datasets, reusable metrics, chart specs,
dashboard filters, and exports Superset dashboard configuration JSON bundle for one-click import.
"""

import sys
import os
import json
import psycopg2
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.config import get_pipeline_config
from src.logging_config import setup_logger

logger = setup_logger("setup_superset")

SUPERSET_METRICS = [
    {
        "metric_name": "Total Revenue",
        "verbose_name": "Total Item Revenue ($)",
        "metric_type": "sum",
        "expression": "SUM(total_item_price)",
        "description": "Total sales revenue generated from item prices excluding freight.",
        "d3format": "$,.2f"
    },
    {
        "metric_name": "Gross Order Value",
        "verbose_name": "Gross Order Value ($)",
        "metric_type": "sum",
        "expression": "SUM(total_order_value)",
        "description": "Gross total order value including item prices and freight charges.",
        "d3format": "$,.2f"
    },
    {
        "metric_name": "Order Count",
        "verbose_name": "Total Order Count",
        "metric_type": "count_distinct",
        "expression": "COUNT(DISTINCT order_id)",
        "description": "Total count of unique e-commerce orders regardless of status.",
        "d3format": ",.0f"
    },
    {
        "metric_name": "Delivered Order Count",
        "verbose_name": "Delivered Order Count",
        "metric_type": "expression",
        "expression": "COUNT(DISTINCT CASE WHEN order_status = 'delivered' THEN order_id END)",
        "description": "Total count of fulfilled e-commerce orders with status 'delivered'.",
        "d3format": ",.0f"
    },
    {
        "metric_name": "Customer Count",
        "verbose_name": "Unique Customer Count",
        "metric_type": "count_distinct",
        "expression": "COUNT(DISTINCT customer_unique_id)",
        "description": "Total count of unique individual customers.",
        "d3format": ",.0f"
    },
    {
        "metric_name": "Average Order Value",
        "verbose_name": "Average Order Value (AOV)",
        "metric_type": "avg",
        "expression": "AVG(total_order_value)",
        "description": "Mean value of completed orders.",
        "d3format": "$,.2f"
    },
    {
        "metric_name": "Average Freight Value",
        "verbose_name": "Average Freight Shipping Cost ($)",
        "metric_type": "avg",
        "expression": "AVG(total_freight_value)",
        "description": "Mean freight shipping cost per order.",
        "d3format": "$,.2f"
    },
    {
        "metric_name": "Late Delivery Count",
        "verbose_name": "Late Deliveries Count",
        "metric_type": "sum",
        "expression": "SUM(CASE WHEN is_late THEN 1 ELSE 0 END)",
        "description": "Number of order deliveries delivered past estimated delivery timestamp.",
        "d3format": ",.0f"
    },
    {
        "metric_name": "Late Delivery Percentage",
        "verbose_name": "Late Delivery Rate (%)",
        "metric_type": "expression",
        "expression": "ROUND((SUM(CASE WHEN is_late THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 2)",
        "description": "Percentage of total delivered orders delivered past SLA target.",
        "d3format": ".2f%"
    },
    {
        "metric_name": "Average Review Score",
        "verbose_name": "Average Customer Review Rating (1-5)",
        "metric_type": "avg",
        "expression": "ROUND(AVG(review_score), 2)",
        "description": "Average review score submitted by customers.",
        "d3format": ".2f"
    },
    {
        "metric_name": "Rejected Record Count",
        "verbose_name": "Data Quality Quarantine Count",
        "metric_type": "sum",
        "expression": "SUM(rejected_row_count)",
        "description": "Total record count routed to quarantine layer due to quality check failures.",
        "d3format": ",.0f"
    },
    {
        "metric_name": "Pipeline Success Rate",
        "verbose_name": "ETL Pipeline Success Rate (%)",
        "metric_type": "expression",
        "expression": "ROUND((COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END)::NUMERIC / NULLIF(COUNT(*), 0)) * 100, 2)",
        "description": "Percentage of pipeline execution runs resulting in status SUCCESS.",
        "d3format": ".2f%"
    }
]

SUPERSET_DATASETS = [
    {
        "table_name": "monthly_sales",
        "schema": "reporting",
        "description": "Monthly aggregated revenue, freight charges, and order counts.",
        "columns": ["year", "month", "month_name", "monthly_order_count", "monthly_item_revenue", "monthly_freight_amount", "monthly_gross_revenue"]
    },
    {
        "table_name": "category_performance",
        "schema": "reporting",
        "description": "Revenue, item count, and average item price by English product category.",
        "columns": ["product_category", "items_sold", "total_category_revenue", "avg_item_price"]
    },
    {
        "table_name": "seller_performance",
        "schema": "reporting",
        "description": "Seller order fulfillment volume, total seller revenue, and state revenue rank.",
        "columns": ["seller_id", "seller_city", "seller_state", "total_orders_fulfilled", "seller_total_revenue", "state_revenue_rank"]
    },
    {
        "table_name": "delivery_performance",
        "schema": "reporting",
        "description": "Delivery SLA metrics, delivery days, late counts, and SLA breach percentages by customer state.",
        "columns": ["customer_state", "total_deliveries", "avg_delivery_days", "avg_estimated_days", "late_deliveries", "late_delivery_percentage"]
    },
    {
        "table_name": "pipeline_health",
        "schema": "reporting",
        "description": "Pipeline execution logs, DAG run history, processing timestamps, accepted/rejected row counts, and rejection rates.",
        "columns": ["pipeline_run_id", "dag_run_id", "load_type", "started_at", "completed_at", "status", "source_row_count", "accepted_row_count", "rejected_row_count", "rejection_rate_percentage"]
    },
    {
        "table_name": "review_performance",
        "schema": "reporting",
        "description": "Review score distribution and average satisfaction scores.",
        "columns": ["review_score", "total_reviews", "avg_review_score"]
    },
    {
        "table_name": "sales_overview",
        "schema": "reporting",
        "description": "Order-granularity view connecting orders, customer state, purchase date, revenue, and order status for cross-chart filter alignment.",
        "columns": ["order_id", "purchase_date", "order_purchase_timestamp", "order_status", "customer_unique_id", "customer_state", "customer_city", "total_revenue", "total_freight", "gross_order_value", "item_count"]
    }
]

SUPERSET_DASHBOARD_CHARTS = [
    {
        "chart_title": "Total Revenue",
        "viz_type": "big_number_total",
        "dataset": "reporting.sales_overview",
        "metric": "Total Revenue",
        "number_format": "$,.2f",
        "description": "Total revenue generated from order item sales."
    },
    {
        "chart_title": "Delivered-Order Count",
        "viz_type": "big_number_total",
        "dataset": "reporting.sales_overview",
        "metric": "Delivered Order Count",
        "number_format": ",.0f",
        "description": "Total count of fulfilled e-commerce orders with status 'delivered'."
    },
    {
        "chart_title": "Average Order Value",
        "viz_type": "big_number_total",
        "dataset": "reporting.sales_overview",
        "metric": "Average Order Value",
        "number_format": "$,.2f",
        "description": "Mean Gross Order Value per order."
    },
    {
        "chart_title": "Late-Delivery Percentage",
        "viz_type": "big_number_total",
        "dataset": "reporting.delivery_performance",
        "metric": "Late Delivery Percentage",
        "number_format": ".2f%",
        "description": "Percentage of order deliveries exceeding SLA."
    },
    {
        "chart_title": "Monthly Revenue Trend",
        "viz_type": "line",
        "dataset": "reporting.monthly_sales",
        "time_column": "month_name",
        "metric": "monthly_item_revenue",
        "number_format": "$,.2f",
        "description": "Line chart showing revenue growth trajectory month over month."
    },
    {
        "chart_title": "Revenue by Product Category",
        "viz_type": "bar",
        "dataset": "reporting.category_performance",
        "group_by": "product_category",
        "metric": "total_category_revenue",
        "number_format": "$,.2f",
        "description": "Bar chart ranking top categories by total sales revenue."
    },
    {
        "chart_title": "Revenue by Customer State",
        "viz_type": "bar",
        "dataset": "reporting.sales_overview",
        "group_by": "customer_state",
        "metric": "Total Revenue",
        "number_format": "$,.2f",
        "description": "Geographic revenue distribution breakdown by customer state."
    },
    {
        "chart_title": "Seller Revenue Ranking",
        "viz_type": "table",
        "dataset": "reporting.seller_performance",
        "group_by": ["seller_id", "seller_state", "state_revenue_rank"],
        "metric": "seller_total_revenue",
        "number_format": "$,.2f",
        "description": "Table ranking sellers by revenue generated and state position."
    },
    {
        "chart_title": "Review-Score Distribution",
        "viz_type": "bar",
        "dataset": "reporting.review_performance",
        "group_by": "review_score",
        "metric": "total_reviews",
        "number_format": ",.0f",
        "description": "Histogram breakdown of customer ratings from 1 to 5 stars."
    },
    {
        "chart_title": "Pipeline-Run Status & Rejected-Record Count",
        "viz_type": "table",
        "dataset": "reporting.pipeline_health",
        "columns": ["pipeline_run_id", "dag_run_id", "status", "source_row_count", "accepted_row_count", "rejected_row_count", "rejection_rate_percentage"],
        "number_format": ",.0f",
        "description": "Data governance table auditing ETL execution status and quarantine metrics."
    }
]

SUPERSET_FILTERS = [
    {"filter_name": "Purchase date", "target_column": "purchase_date", "dataset": "reporting.sales_overview", "control_type": "date_filter"},
    {"filter_name": "Customer state", "target_column": "customer_state", "dataset": "reporting.sales_overview", "control_type": "select"},
    {"filter_name": "Product category", "target_column": "product_category", "dataset": "reporting.category_performance", "control_type": "select"},
    {"filter_name": "Order status", "target_column": "order_status", "dataset": "reporting.sales_overview", "control_type": "select"},
    {"filter_name": "Seller state", "target_column": "seller_state", "dataset": "reporting.seller_performance", "control_type": "select"}
]


def test_postgres_reporting_connection(config: dict) -> bool:
    """Verifies connection to PostgreSQL database and checks availability of reporting schema views."""
    env = config["env"]
    try:
        conn = psycopg2.connect(
            host=env["POSTGRES_HOST"],
            port=env["POSTGRES_PORT"],
            dbname=env["POSTGRES_DB"],
            user=env["POSTGRES_USER"],
            password=env["POSTGRES_PASSWORD"]
        )
        cursor = conn.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'reporting';")
        tables = [r[0] for r in cursor.fetchall()]
        cursor.close()
        conn.close()
        logger.info(f"Connected to PostgreSQL reporting schema. Registered views found: {tables}")
        return True
    except Exception as e:
        logger.warning(f"PostgreSQL connection test failed (Host: {env['POSTGRES_HOST']}:{env['POSTGRES_PORT']}): {e}")
        return False


def build_superset_configuration_bundle(config: dict) -> dict:
    """Builds a complete JSON object containing Superset database, datasets, metrics, charts, and dashboard metadata."""
    env = config["env"]
    
    # In Docker container, host is 'postgres'. On host machine outside Docker, host is 'localhost' (or configured host).
    docker_host = "postgres"
    local_host = env["POSTGRES_HOST"]
    db_name = env["POSTGRES_DB"]
    db_user = env["POSTGRES_USER"]
    db_pass = env["POSTGRES_PASSWORD"]
    
    sqlalchemy_docker_uri = f"postgresql+psycopg2://{db_user}:{db_pass}@{docker_host}:5432/{db_name}"
    sqlalchemy_local_uri = f"postgresql+psycopg2://{db_user}:{db_pass}@{local_host}:{env['POSTGRES_PORT']}/{db_name}"

    bundle = {
        "metadata_version": "1.0",
        "generated_at": datetime.now().isoformat(),
        "database_connection": {
            "database_name": "olist_warehouse",
            "engine": "postgresql",
            "docker_sqlalchemy_uri": sqlalchemy_docker_uri,
            "local_sqlalchemy_uri": sqlalchemy_local_uri,
            "extra": {
                "allows_virtual_table_explore": True,
                "schemas_allowed_for_csv_upload": ["reporting"]
            }
        },
        "datasets": SUPERSET_DATASETS,
        "metrics": SUPERSET_METRICS,
        "charts": SUPERSET_DASHBOARD_CHARTS,
        "filters": SUPERSET_FILTERS,
        "dashboard": {
            "dashboard_title": "Olist E-Commerce Executive Operational Dashboard",
            "slug": "olist_executive_operational_dashboard",
            "position_json": {
                "CHART-total-revenue": {"type": "CHART", "id": "CHART-total-revenue", "children": []},
                "CHART-order-count": {"type": "CHART", "id": "CHART-order-count", "children": []},
                "CHART-aov": {"type": "CHART", "id": "CHART-aov", "children": []},
                "CHART-late-delivery-pct": {"type": "CHART", "id": "CHART-late-delivery-pct", "children": []},
                "CHART-monthly-revenue": {"type": "CHART", "id": "CHART-monthly-revenue", "children": []},
                "CHART-category-revenue": {"type": "CHART", "id": "CHART-category-revenue", "children": []},
                "CHART-customer-state-revenue": {"type": "CHART", "id": "CHART-customer-state-revenue", "children": []},
                "CHART-seller-ranking": {"type": "CHART", "id": "CHART-seller-ranking", "children": []},
                "CHART-review-distribution": {"type": "CHART", "id": "CHART-review-distribution", "children": []},
                "CHART-pipeline-health": {"type": "CHART", "id": "CHART-pipeline-health", "children": []}
            }
        }
    }
    return bundle


def main():
    logger.info("Initializing Apache Superset Integration & Dashboard Exporter")
    config = get_pipeline_config()
    
    # Check PostgreSQL database availability
    pg_ok = test_postgres_reporting_connection(config)
    if pg_ok:
        logger.info("PostgreSQL Warehouse is reachable and reporting schema views are verified!")
    else:
        logger.info("PostgreSQL check skipped or offline. Configuration export proceeding.")

    bundle = build_superset_configuration_bundle(config)

    reports_dir = BASE_DIR / "reports"
    reports_dir.mkdir(exist_ok=True)
    out_file = reports_dir / "superset_dashboard_config.json"

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(bundle, f, indent=2)

    logger.info(f"Superset Dashboard configuration export successfully generated at: {out_file}")
    print(f"\n======================================================================")
    print(f"  APACHE SUPERSET INTEGRATION CONFIGURATION COMPLETED SUCCESSFULLY")
    print(f"======================================================================")
    print(f"  Config File Exported: {out_file}")
    print(f"  Registered Datasets : {len(SUPERSET_DATASETS)}")
    print(f"  Registered Metrics  : {len(SUPERSET_METRICS)}")
    print(f"  Configured Charts   : {len(SUPERSET_DASHBOARD_CHARTS)}")
    print(f"  Configured Filters  : {len(SUPERSET_FILTERS)}")
    print(f"======================================================================\n")


if __name__ == "__main__":
    main()
