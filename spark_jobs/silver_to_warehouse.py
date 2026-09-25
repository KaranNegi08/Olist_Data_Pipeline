"""
PySpark ETL Job: Transforms Silver Parquet data into Kimball Star Schema dimensions and facts,
and loads them into PostgreSQL Data Warehouse via JDBC.
"""

import sys
import os
from pathlib import Path
from datetime import datetime
from uuid import uuid4
import argparse

# Ensure JAVA_HOME is set for PySpark on Windows
if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ["JAVA_HOME"]):
    if os.path.exists(r"C:\java\jdk"):
        os.environ["JAVA_HOME"] = r"C:\java\jdk"
        os.environ["PATH"] = r"C:\java\jdk\bin;" + os.environ.get("PATH", "")

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.config import get_pipeline_config
from src.logging_config import setup_logger
from src.spark_utils import create_spark_session

logger = setup_logger("silver_to_warehouse")


def write_to_postgres(df, table_name: str, config: dict, mode: str = "overwrite", run_id: str = "N/A"):
    """Writes a Spark DataFrame directly to a PostgreSQL table using JDBC if configured."""
    env = config["env"]
    jdbc_url = env["POSTGRES_JDBC_URL"]
    user = env["POSTGRES_USER"]
    password = env["POSTGRES_PASSWORD"]
    driver = config["database"]["driver"]

    jdbc_options = {
        "url": jdbc_url,
        "driver": driver,
        "user": user,
        "password": password,
        "dbtable": table_name,
        "batchsize": "5000",
        "truncate": "true",
        "cascadeTruncate": "true"
    }

    count = df.count()
    try:
        logger.info(
            f"Writing records to PostgreSQL table (Mode: {mode})",
            run_id=run_id,
            task_name="silver_to_warehouse",
            target=table_name,
            record_count=count
        )
        (
            df.write
            .format("jdbc")
            .options(**jdbc_options)
            .mode(mode)
            .save()
        )
    except Exception as e:
        logger.error(
            f"Database write failed for {table_name}: {str(e)}",
            run_id=run_id,
            task_name="silver_to_warehouse",
            target=table_name,
            record_count=count
        )
        raise e

def load_silver_dataset(spark: SparkSession, silver_dir: Path, dataset_name: str, required: bool = True):
    """Dynamically loads a Silver Parquet dataset by key from config."""
    dataset_path = silver_dir / dataset_name
    if not dataset_path.exists() or not any(dataset_path.rglob("*.parquet")):
        if required:
            raise FileNotFoundError(f"Required Silver dataset missing or empty at: {dataset_path}")
        logger.warning(
            f"Optional Silver dataset not found or empty at: {dataset_path}",
            task_name="silver_to_warehouse",
            target=dataset_name
        )
        return None
    try:
        return spark.read.parquet(str(dataset_path))
    except Exception as e:
        if not required:
            return None
        raise e


def main():
    parser = argparse.ArgumentParser(description="Olist Silver to Warehouse PySpark Job")
    parser.add_argument("--run-id", type=str, default=str(uuid4()), help="Pipeline Run ID")
    parser.add_argument("--load-type", type=str, default="full", help="Load Type: full or incremental")
    args = parser.parse_args()

    run_id = args.run_id
    config = get_pipeline_config()
    silver_dir = BASE_DIR / config["storage"]["silver_path"]

    logger.info(
        f"Starting Silver to Warehouse PySpark Job (Mode: {args.load_type})",
        run_id=run_id,
        task_name="silver_to_warehouse"
    )

    spark = create_spark_session("olist-silver-to-warehouse")
    spark.sparkContext.setLogLevel("WARN")

    tables_config = config.get("tables", {})

    # Dynamically load Silver Parquet datasets based on table config keys
    datasets = {}
    optional_tables = {"category_translation", "geolocation", "order_reviews"}
    for table_key in tables_config.keys():
        is_required = (table_key not in optional_tables)
        ds = load_silver_dataset(spark, silver_dir, table_key, required=is_required)
        if ds is not None:
            datasets[table_key] = ds

    orders = datasets["orders"]
    customers = datasets["customers"]
    products = datasets["products"]
    sellers = datasets["sellers"]
    order_items = datasets["order_items"]
    order_payments = datasets["order_payments"]
    cat_trans = datasets.get("category_translation")

    # Filter for incremental batch if requested (Read last watermark or fallback to batch_date_split)
    if args.load_type == "incremental":
        split_date = config["pipeline"]["batch_date_split"]
        wm_val = None
        try:
            from src.audit import get_last_watermark
            last_wm = get_last_watermark()
            if last_wm and last_wm.year <= 2018:
                wm_val = last_wm.isoformat()
        except Exception:
            pass
            
        cutoff_val = wm_val if wm_val else split_date
        logger.info(
            f"Applying incremental filter: order_purchase_timestamp >= {cutoff_val}",
            run_id=run_id,
            task_name="silver_to_warehouse",
            target="orders"
        )
        orders = orders.filter(F.col("order_purchase_timestamp") >= F.lit(cutoff_val))

    # 1. DIMENSIONS
    # dim_customer
    w_cust = Window.orderBy("customer_id")
    dim_customer = (
        customers
        .select(
            F.col("customer_id"),
            F.col("customer_unique_id"),
            F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
            F.col("customer_city").alias("city"),
            F.col("customer_state").alias("state")
        )
        .withColumn("customer_key", F.row_number().over(w_cust))
    )

    # dim_product (with English translations)
    w_prod = Window.orderBy("product_id")
    if cat_trans:
        dim_product = (
            products.join(cat_trans, on="product_category_name", how="left")
            .select(
                F.col("product_id"),
                F.col("product_category_name").alias("category_name_pt"),
                F.coalesce(F.col("product_category_name_english"), F.col("product_category_name")).alias("category_name_en"),
                F.col("product_name_lenght").alias("name_length"),
                F.col("product_description_lenght").alias("description_length"),
                F.col("product_photos_qty").alias("photos_qty"),
                F.col("product_weight_g").alias("weight_g"),
                F.col("product_length_cm").alias("length_cm"),
                F.col("product_height_cm").alias("height_cm"),
                F.col("product_width_cm").alias("width_cm")
            )
            .withColumn("product_key", F.row_number().over(w_prod))
        )
    else:
        dim_product = (
            products.select(
                F.col("product_id"),
                F.col("product_category_name").alias("category_name_pt"),
                F.col("product_category_name").alias("category_name_en"),
                F.col("product_name_lenght").alias("name_length"),
                F.col("product_description_lenght").alias("description_length"),
                F.col("product_photos_qty").alias("photos_qty"),
                F.col("product_weight_g").alias("weight_g"),
                F.col("product_length_cm").alias("length_cm"),
                F.col("product_height_cm").alias("height_cm"),
                F.col("product_width_cm").alias("width_cm")
            )
            .withColumn("product_key", F.row_number().over(w_prod))
        )

    # dim_seller
    w_sell = Window.orderBy("seller_id")
    dim_seller = (
        sellers.select(
            F.col("seller_id"),
            F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
            F.col("seller_city").alias("city"),
            F.col("seller_state").alias("state")
        )
        .withColumn("seller_key", F.row_number().over(w_sell))
    )

    # 2. FACTS
    # Aggregate order_items for fact_order
    order_item_agg = (
        order_items
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("total_item_price"),
            F.sum("freight_value").alias("total_freight_value"),
            F.count("order_item_id").alias("item_count")
        )
    )

    # Order status key mapping
    order_status_mapping = (
        F.when(F.col("order_status") == "delivered", 1)
        .when(F.col("order_status") == "shipped", 2)
        .when(F.col("order_status") == "canceled", 3)
        .when(F.col("order_status") == "invoiced", 4)
        .when(F.col("order_status") == "processing", 5)
        .when(F.col("order_status") == "created", 6)
        .when(F.col("order_status") == "approved", 7)
        .when(F.col("order_status") == "unavailable", 8)
        .otherwise(None)
    )

    # fact_order
    fact_order = (
        orders
        .join(order_item_agg, on="order_id", how="left")
        .join(dim_customer.select("customer_id", "customer_key"), on="customer_id", how="left")
        .withColumn("order_status_key", order_status_mapping)
        .withColumn("total_item_price", F.coalesce(F.col("total_item_price"), F.lit(0.0)))
        .withColumn("total_freight_value", F.coalesce(F.col("total_freight_value"), F.lit(0.0)))
        .withColumn("total_order_value", F.col("total_item_price") + F.col("total_freight_value"))
        .withColumn("item_count", F.coalesce(F.col("item_count"), F.lit(0)))
        .withColumn("date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
        .select(
            "order_id",
            "customer_id",
            "customer_key",
            "order_status",
            "order_status_key",
            "date_key",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "total_item_price",
            "total_freight_value",
            "total_order_value",
            "item_count"
        )
    )

    # fact_order_item
    fact_order_item = (
        order_items
        .join(dim_product.select("product_id", "product_key"), on="product_id", how="inner")
        .join(dim_seller.select("seller_id", "seller_key"), on="seller_id", how="inner")
        .select(
            "order_id",
            "order_item_id",
            "product_key",
            "seller_key",
            "shipping_limit_date",
            "price",
            "freight_value"
        )
    )

    # fact_payment
    payment_type_mapping = (
        F.when(F.col("payment_type") == "credit_card", 1)
        .when(F.col("payment_type") == "boleto", 2)
        .when(F.col("payment_type") == "voucher", 3)
        .when(F.col("payment_type") == "debit_card", 4)
        .otherwise(5)
    )

    fact_payment = (
        order_payments
        .withColumn("payment_type_key", payment_type_mapping)
        .select(
            "order_id",
            "payment_sequential",
            "payment_type_key",
            "payment_installments",
            "payment_value"
        )
    )

    # fact_delivery
    fact_delivery = (
        orders
        .filter(F.col("order_delivered_customer_date").isNotNull())
        .join(dim_customer.select("customer_id", "customer_key"), on="customer_id", how="left")
        .withColumn("date_key", F.date_format("order_purchase_timestamp", "yyyyMMdd").cast("int"))
        .withColumn(
            "delivery_days",
            (F.col("order_delivered_customer_date").cast("long") - F.col("order_purchase_timestamp").cast("long")) / 86400.0
        )
        .withColumn(
            "estimated_delivery_days",
            (F.col("order_estimated_delivery_date").cast("long") - F.col("order_purchase_timestamp").cast("long")) / 86400.0
        )
        .withColumn(
            "delay_days",
            F.when(
                F.to_date("order_delivered_customer_date") > F.to_date("order_estimated_delivery_date"),
                (F.to_date("order_delivered_customer_date").cast("long") - F.to_date("order_estimated_delivery_date").cast("long")) / 86400.0
            ).otherwise(F.lit(0.0))
        )
        .withColumn("is_late", F.to_date("order_delivered_customer_date") > F.to_date("order_estimated_delivery_date"))
        .select(
            "order_id",
            "customer_id",
            "customer_key",
            "date_key",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "delivery_days",
            "estimated_delivery_days",
            "delay_days",
            "is_late"
        )
    )

    # fact_review
    fact_review = None
    if "order_reviews" in datasets:
        order_reviews = datasets["order_reviews"]
        fact_review = (
            order_reviews
            .select(
                F.col("review_id"),
                F.col("order_id"),
                F.col("review_score").cast("int"),
                F.col("review_creation_date"),
                F.col("review_answer_timestamp")
            )
        )

    tables_info = [
        ("dim_customer", dim_customer),
        ("dim_product", dim_product),
        ("dim_seller", dim_seller),
        ("fact_order", fact_order),
        ("fact_order_item", fact_order_item),
        ("fact_payment", fact_payment),
        ("fact_delivery", fact_delivery)
    ]
    if fact_review is not None:
        tables_info.append(("fact_review", fact_review))


    for tbl_name, tbl_df in tables_info:
        cnt = tbl_df.count()
        logger.info(
            f"Transformed table {tbl_name}",
            run_id=run_id,
            task_name="silver_to_warehouse",
            target=tbl_name,
            record_count=cnt
        )
        write_to_postgres(tbl_df, f"warehouse.{tbl_name}", config, mode="overwrite", run_id=run_id)

    spark.stop()
    logger.info(
        "Silver to Warehouse PySpark Transformation Job completed successfully!",
        run_id=run_id,
        task_name="silver_to_warehouse"
    )


if __name__ == "__main__":
    main()
