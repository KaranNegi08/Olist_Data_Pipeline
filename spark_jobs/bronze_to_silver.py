"""
PySpark ETL Job: Ingests Bronze CSV files, enforces explicit schemas, runs data quality checks,
routes invalid rows to Quarantine, deduplicates, and writes clean datasets to Silver Parquet storage.
"""

import sys
import os
import json
from datetime import datetime
from pathlib import Path
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
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
)
from src.config import get_pipeline_config
from src.logging_config import setup_logger
from src.manifest import discover_and_build_manifest
from src.spark_utils import create_spark_session
from src.referential_integrity import validate_referential_integrity

logger = setup_logger("bronze_to_silver")

# 1. EXPLICIT SCHEMAS & CONFIGURATION
SCHEMAS = {
    "customers": (
        StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_unique_id", StringType(), True),
            StructField("customer_zip_code_prefix", IntegerType(), True),
            StructField("customer_city", StringType(), True),
            StructField("customer_state", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["customer_id"]
    ),

    "orders": (
        StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_purchase_timestamp", TimestampType(), True),
            StructField("order_approved_at", TimestampType(), True),
            StructField("order_delivered_carrier_date", TimestampType(), True),
            StructField("order_delivered_customer_date", TimestampType(), True),
            StructField("order_estimated_delivery_date", TimestampType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["order_id"]
    ),

    "order_items": (
        StructType([
            StructField("order_id", StringType(), True),
            StructField("order_item_id", IntegerType(), True),
            StructField("product_id", StringType(), True),
            StructField("seller_id", StringType(), True),
            StructField("shipping_limit_date", TimestampType(), True),
            StructField("price", DoubleType(), True),
            StructField("freight_value", DoubleType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["order_id", "order_item_id"]
    ),

    "order_payments": (
        StructType([
            StructField("order_id", StringType(), True),
            StructField("payment_sequential", IntegerType(), True),
            StructField("payment_type", StringType(), True),
            StructField("payment_installments", IntegerType(), True),
            StructField("payment_value", DoubleType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["order_id", "payment_sequential"]
    ),

    "order_reviews": (
        StructType([
            StructField("review_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("review_score", IntegerType(), True),
            StructField("review_comment_title", StringType(), True),
            StructField("review_comment_message", StringType(), True),
            StructField("review_creation_date", TimestampType(), True),
            StructField("review_answer_timestamp", TimestampType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["review_id", "order_id"]
    ),

    "products": (
        StructType([
            StructField("product_id", StringType(), True),
            StructField("product_category_name", StringType(), True),
            StructField("product_name_lenght", IntegerType(), True),
            StructField("product_description_lenght", IntegerType(), True),
            StructField("product_photos_qty", IntegerType(), True),
            StructField("product_weight_g", DoubleType(), True),
            StructField("product_length_cm", DoubleType(), True),
            StructField("product_height_cm", DoubleType(), True),
            StructField("product_width_cm", DoubleType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["product_id"]
    ),

    "sellers": (
        StructType([
            StructField("seller_id", StringType(), True),
            StructField("seller_zip_code_prefix", IntegerType(), True),
            StructField("seller_city", StringType(), True),
            StructField("seller_state", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["seller_id"]
    ),

    "category_translation": (
        StructType([
            StructField("product_category_name", StringType(), True),
            StructField("product_category_name_english", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["product_category_name"]
    ),

    "geolocation": (
        StructType([
            StructField("geolocation_zip_code_prefix", IntegerType(), True),
            StructField("geolocation_lat", DoubleType(), True),
            StructField("geolocation_lng", DoubleType(), True),
            StructField("geolocation_city", StringType(), True),
            StructField("geolocation_state", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ]),
        ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"]
    )
}


# Dynamically load table file map from config/table_schemas.yml configuration
_pipeline_config = get_pipeline_config()
_tables_config = _pipeline_config.get("tables", {})
TABLE_FILE_MAP = {
    tbl_name: tbl_info["file_name"]
    for tbl_name, tbl_info in _tables_config.items()
    if "file_name" in tbl_info
}

from typing import Tuple, Optional, Dict
from pyspark.sql import Column, DataFrame

def get_rejection_expression(table_name: str) -> Column:
    """
    Returns PySpark Column validation rules dynamically enforcing strict Quarantine Layer logic.

    Args:
        table_name (str): Name of the dataset table being processed.

    Returns:
        Column: PySpark Column expression producing rejection_reason string or NULL for clean rows.
    """
    corrupt_check = F.when(F.col("_corrupt_record").isNotNull(), F.lit("CORRUPT_CSV_ROW"))

    if table_name == "customers":
        return corrupt_check \
            .when(F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""), F.lit("MISSING_CUSTOMER_ID")) \
            .when(F.col("customer_unique_id").isNull() | (F.trim(F.col("customer_unique_id")) == ""), F.lit("MISSING_CUSTOMER_UNIQUE_ID")) \
            .when(F.col("customer_city").isNull() | (F.trim(F.col("customer_city")) == ""), F.lit("MISSING_CUSTOMER_CITY")) \
            .when(F.col("customer_state").isNull() | (F.trim(F.col("customer_state")) == ""), F.lit("MISSING_CUSTOMER_STATE")) \
            .when(F.col("customer_zip_code_prefix").isNull() | (F.col("customer_zip_code_prefix") < 0) | (F.col("customer_zip_code_prefix") > 99999), F.lit("INVALID_ZIP_CODE_PREFIX")) \
            .when(F.length(F.trim(F.col("customer_state"))) != 2, F.lit("INVALID_CUSTOMER_STATE"))

    elif table_name == "sellers":
        return corrupt_check \
            .when(F.col("seller_id").isNull() | (F.trim(F.col("seller_id")) == ""), F.lit("MISSING_SELLER_ID")) \
            .when(F.col("seller_city").isNull() | (F.trim(F.col("seller_city")) == ""), F.lit("MISSING_SELLER_CITY")) \
            .when(F.col("seller_state").isNull() | (F.trim(F.col("seller_state")) == ""), F.lit("MISSING_SELLER_STATE")) \
            .when(F.col("seller_zip_code_prefix").isNull() | (F.col("seller_zip_code_prefix") < 0) | (F.col("seller_zip_code_prefix") > 99999), F.lit("INVALID_ZIP_CODE_PREFIX")) \
            .when(F.length(F.trim(F.col("seller_state"))) != 2, F.lit("INVALID_SELLER_STATE"))

    elif table_name == "products":
        return corrupt_check \
            .when(F.col("product_id").isNull() | (F.trim(F.col("product_id")) == ""), F.lit("MISSING_PRODUCT_ID")) \
            .when(F.col("product_weight_g").isNotNull() & (F.col("product_weight_g") < 0), F.lit("NEGATIVE_PRODUCT_WEIGHT")) \
            .when(F.col("product_length_cm").isNotNull() & (F.col("product_length_cm") < 0), F.lit("NEGATIVE_PRODUCT_LENGTH")) \
            .when(F.col("product_height_cm").isNotNull() & (F.col("product_height_cm") < 0), F.lit("NEGATIVE_PRODUCT_HEIGHT")) \
            .when(F.col("product_width_cm").isNotNull() & (F.col("product_width_cm") < 0), F.lit("NEGATIVE_PRODUCT_WIDTH"))

    elif table_name == "geolocation":
        return corrupt_check \
            .when(F.col("geolocation_city").isNull() | (F.trim(F.col("geolocation_city")) == ""), F.lit("MISSING_GEOLOCATION_CITY")) \
            .when(F.col("geolocation_state").isNull() | (F.trim(F.col("geolocation_state")) == ""), F.lit("MISSING_GEOLOCATION_STATE")) \
            .when(F.col("geolocation_zip_code_prefix").isNull() | (F.col("geolocation_zip_code_prefix") < 0) | (F.col("geolocation_zip_code_prefix") > 99999), F.lit("INVALID_ZIP_CODE_PREFIX")) \
            .when(F.col("geolocation_lat").isNull() | (F.col("geolocation_lat") < -90) | (F.col("geolocation_lat") > 90), F.lit("INVALID_LATITUDE")) \
            .when(F.col("geolocation_lng").isNull() | (F.col("geolocation_lng") < -180) | (F.col("geolocation_lng") > 180), F.lit("INVALID_LONGITUDE")) \
            .when(F.length(F.trim(F.col("geolocation_state"))) != 2, F.lit("INVALID_GEOLOCATION_STATE"))

    elif table_name == "orders":
        valid_statuses = ["approved", "canceled", "created", "delivered", "invoiced", "processing", "shipped", "unavailable"]
        return corrupt_check \
            .when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), F.lit("MISSING_ORDER_ID")) \
            .when(F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == ""), F.lit("MISSING_CUSTOMER_ID")) \
            .when(F.col("order_status").isNull() | (F.trim(F.col("order_status")) == ""), F.lit("MISSING_ORDER_STATUS")) \
            .when(F.col("order_purchase_timestamp").isNull(), F.lit("INVALID_ORDER_PURCHASE_TIMESTAMP")) \
            .when(~F.col("order_status").isin(valid_statuses), F.lit("INVALID_ORDER_STATUS")) \
            .when((F.col("order_status") == "delivered") & F.col("order_delivered_customer_date").isNull(), F.lit("DELIVERED_WITHOUT_DELIVERY_TIMESTAMP")) \
            .when(F.col("order_approved_at").isNotNull() & (F.col("order_approved_at") < F.col("order_purchase_timestamp")), F.lit("APPROVED_BEFORE_PURCHASE")) \
            .when(F.col("order_delivered_carrier_date").isNotNull() & (F.col("order_delivered_carrier_date") < F.col("order_purchase_timestamp")), F.lit("CARRIER_DELIVERY_BEFORE_PURCHASE")) \
            .when(F.col("order_delivered_customer_date").isNotNull() & F.col("order_delivered_carrier_date").isNotNull() & (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date")), F.lit("CUSTOMER_DELIVERY_BEFORE_CARRIER")) \
            .when(F.col("order_estimated_delivery_date").isNotNull() & (F.col("order_estimated_delivery_date") < F.col("order_purchase_timestamp")), F.lit("ESTIMATED_DELIVERY_BEFORE_PURCHASE"))

    elif table_name == "order_items":
        return corrupt_check \
            .when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), F.lit("MISSING_ORDER_ID")) \
            .when(F.col("order_item_id").isNull(), F.lit("MISSING_ORDER_ITEM_ID")) \
            .when(F.col("product_id").isNull() | (F.trim(F.col("product_id")) == ""), F.lit("MISSING_PRODUCT_ID")) \
            .when(F.col("seller_id").isNull() | (F.trim(F.col("seller_id")) == ""), F.lit("MISSING_SELLER_ID")) \
            .when(F.col("shipping_limit_date").isNull(), F.lit("INVALID_SHIPPING_LIMIT_DATE")) \
            .when(F.col("price").isNull() | (F.col("price") < 0), F.lit("INVALID_PRICE")) \
            .when(F.col("freight_value").isNull() | (F.col("freight_value") < 0), F.lit("INVALID_FREIGHT_VALUE"))

    elif table_name == "order_payments":
        return corrupt_check \
            .when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), F.lit("MISSING_ORDER_ID")) \
            .when(F.col("payment_sequential").isNull(), F.lit("MISSING_PAYMENT_SEQUENTIAL")) \
            .when(F.col("payment_type").isNull() | (F.trim(F.col("payment_type")) == ""), F.lit("MISSING_PAYMENT_TYPE")) \
            .when(F.col("payment_value").isNull() | (F.col("payment_value") < 0), F.lit("INVALID_PAYMENT_VALUE"))

    elif table_name == "order_reviews":
        return corrupt_check \
            .when(F.col("review_id").isNull() | (F.trim(F.col("review_id")) == ""), F.lit("MISSING_REVIEW_ID")) \
            .when(F.col("order_id").isNull() | (F.trim(F.col("order_id")) == ""), F.lit("MISSING_ORDER_ID")) \
            .when(F.col("review_score").isNull() | (~F.col("review_score").isin([1, 2, 3, 4, 5])), F.lit("INVALID_REVIEW_SCORE"))

    elif table_name == "category_translation":
        return corrupt_check \
            .when(F.col("product_category_name").isNull() | (F.trim(F.col("product_category_name")) == ""), F.lit("MISSING_PRODUCT_CATEGORY_NAME")) \
            .when(F.col("product_category_name_english").isNull() | (F.trim(F.col("product_category_name_english")) == ""), F.lit("MISSING_PRODUCT_CATEGORY_NAME_ENGLISH"))

    else:
        return corrupt_check




def process_table(
    spark: SparkSession, 
    table_name: str, 
    bronze_dir: Path, 
    quarantine_dir: Path, 
    run_id: str
) -> Tuple[Optional[DataFrame], Optional[Dict]]:
    """
    Ingests raw Bronze CSV data, evaluates data quality gates, routes invalid rows to Quarantine Parquet,
    deduplicates on primary business keys, and returns clean DataFrames with execution summary stats.

    Args:
        spark (SparkSession): Active PySpark SparkSession instance.
        table_name (str): Name of table being processed (e.g. 'orders', 'customers').
        bronze_dir (Path): Base directory path for Bronze CSV data lake storage.
        quarantine_dir (Path): Base directory path for Quarantine Parquet isolation.
        run_id (str): Unique execution run identifier.

    Returns:
        Tuple[Optional[DataFrame], Optional[Dict]]: Tuple containing (clean_df, execution_stats_dict).
    """
    if table_name not in SCHEMAS:
        return None, None

    schema, dedupe_keys = SCHEMAS[table_name]
    rejection_expr = get_rejection_expression(table_name)
    file_name = TABLE_FILE_MAP.get(table_name, f"olist_{table_name}_dataset.csv")
    file_path = bronze_dir / file_name

    if not file_path.exists():
        matching_files = list(bronze_dir.rglob(file_name))
        if matching_files:
            file_path = matching_files[-1]  # Pick latest ingestion folder
        else:
            logger.warning(
                f"Source CSV file not found: {file_name}",
                run_id=run_id,
                task_name="bronze_to_silver",
                target=file_name,
                record_count=0
            )
            return None, None

    # Read CSV using PERMISSIVE mode, multiLine support, and explicit schema
    raw_df = (
        spark.read
        .option("header", "true")
        .option("multiLine", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(str(file_path))
    )

    # Evaluate Data Quality Rules
    validated_df = raw_df.withColumn("rejection_reason", rejection_expr)

    # Write Rejected Records to Quarantine Layer
    rejected_df = validated_df.filter(F.col("rejection_reason").isNotNull())
    rejected_count = rejected_df.count()
    if rejected_count > 0:
        logger.warning(
            f"Quarantined invalid records for table {table_name}",
            run_id=run_id,
            task_name="bronze_to_silver",
            target=table_name,
            record_count=rejected_count
        )
        out_quarantine = quarantine_dir / table_name
        (
            rejected_df
            .withColumn("source_file_name", F.lit(file_name))
            .withColumn("pipeline_run_id", F.lit(run_id))
            .withColumn("rejected_at", F.current_timestamp())
            .write.mode("append").parquet(str(out_quarantine))
        )

    # Filter Clean Records & Deduplicate
    valid_df = (
        validated_df
        .filter(F.col("rejection_reason").isNull())
        .drop("rejection_reason", "_corrupt_record")
    )
    valid_count = valid_df.count()

    clean_df = valid_df.dropDuplicates(subset=dedupe_keys)
    clean_count = clean_df.count()
    dedup_removed_count = valid_count - clean_count

    if dedup_removed_count > 0:
        logger.info(
            f"Deduplication on {table_name} using key {dedupe_keys}: Removed {dedup_removed_count} duplicate records ({valid_count} -> {clean_count})",
            run_id=run_id,
            task_name="bronze_to_silver",
            target=table_name,
            record_count=dedup_removed_count
        )
    else:
        logger.info(
            f"Deduplication on {table_name} using key {dedupe_keys}: 0 duplicates found",
            run_id=run_id,
            task_name="bronze_to_silver",
            target=table_name,
            record_count=0
        )

    stats = {
        "table_name": table_name,
        "business_key": dedupe_keys,
        "valid_records_before_dedup": valid_count,
        "clean_records_after_dedup": clean_count,
        "duplicate_rows_removed": dedup_removed_count
    }

    return clean_df, stats

def main():
    parser = argparse.ArgumentParser(description="Olist Bronze to Silver PySpark Job")
    parser.add_argument("--run-id", type=str, default=str(uuid4()), help="Pipeline Run ID")
    parser.add_argument("--load-type", type=str, default="full", help="Load Type: full or incremental")
    args = parser.parse_args()

    run_id = args.run_id
    config = get_pipeline_config()
    bronze_dir = BASE_DIR / config["storage"]["bronze_path"]
    silver_dir = BASE_DIR / config["storage"]["silver_path"]
    quarantine_dir = BASE_DIR / config["storage"]["quarantine_path"]

    logger.info(
        f"Starting Bronze to Silver PySpark Job (Load Type: {args.load_type})",
        run_id=run_id,
        task_name="bronze_to_silver"
    )

    # Discover source files and build manifest
    manifest_entries = discover_and_build_manifest(bronze_dir, run_id)
    if not manifest_entries:
        logger.error(
            "No source CSV files discovered in Bronze layer. Exiting ETL job.",
            run_id=run_id,
            task_name="bronze_to_silver",
            record_count=0
        )
        sys.exit(1)

    spark = create_spark_session("olist-bronze-to-silver")
    spark.sparkContext.setLogLevel("WARN")

    # Dynamically process all datasets configured in SCHEMAS and aggregate deduplication stats
    table_order = list(SCHEMAS.keys())
    clean_dfs = {}
    dedup_reports = []
    for table_name in table_order:
        clean_df, stats = process_table(spark, table_name, bronze_dir, quarantine_dir, run_id)
        if clean_df is not None:
            clean_dfs[table_name] = clean_df
            dedup_reports.append(stats)

    # Apply Incremental vs Initial Load Batch Filtering (Read last watermark or fallback to batch_date_split)
    load_type = args.load_type.lower()
    split_date = config.get("pipeline", {}).get("batch_date_split", "2018-06-01")
    
    if load_type in ("initial", "incremental") and "orders" in clean_dfs:
        if load_type == "initial":
            logger.info(f"Applying INITIAL load filter: orders purchased BEFORE {split_date}", run_id=run_id, task_name="bronze_to_silver")
            clean_dfs["orders"] = clean_dfs["orders"].filter(F.col("order_purchase_timestamp") < F.lit(split_date))
        else:
            wm_val = None
            try:
                from src.audit import get_last_watermark
                last_wm = get_last_watermark()
                if last_wm and last_wm.year <= 2018:
                    wm_val = last_wm.isoformat()
            except Exception:
                pass
                
            cutoff_val = wm_val if wm_val else split_date
            logger.info(f"Applying INCREMENTAL load filter: orders purchased ON/AFTER {cutoff_val}", run_id=run_id, task_name="bronze_to_silver")
            clean_dfs["orders"] = clean_dfs["orders"].filter(F.col("order_purchase_timestamp") >= F.lit(cutoff_val))
            
        # Consistently filter related child entities (items, payments, reviews) to maintain referential integrity
        valid_order_ids = clean_dfs["orders"].select("order_id")
        for child_tbl in ["order_items", "order_payments", "order_reviews"]:
            if child_tbl in clean_dfs:
                clean_dfs[child_tbl] = clean_dfs[child_tbl].join(valid_order_ids, on="order_id", how="inner")

    # Enforce Referential-Integrity Validation (left-anti joins)
    clean_dfs, ri_counts = validate_referential_integrity(clean_dfs, quarantine_dir, run_id)

    # Write clean and referentially-valid DataFrames to Silver Parquet
    for table_name, clean_df in clean_dfs.items():
        out_silver = silver_dir / table_name
        final_count = clean_df.count()
        if table_name == "orders":
            clean_df = (
                clean_df
                .withColumn("purchase_year", F.year("order_purchase_timestamp"))
                .withColumn("purchase_month", F.month("order_purchase_timestamp"))
            )
            clean_df.write.mode("overwrite").partitionBy("purchase_year", "purchase_month").parquet(str(out_silver))
        else:
            clean_df.write.mode("overwrite").parquet(str(out_silver))

        logger.info(
            f"Clean Parquet dataset written to Silver layer",
            run_id=run_id,
            task_name="bronze_to_silver",
            target=table_name,
            record_count=final_count
        )

    # Save Deduplication & Referential Integrity Report Artifact
    try:
        reports_dir = BASE_DIR / "reports" / "data_quality"
        reports_dir.mkdir(parents=True, exist_ok=True)
        report_file = reports_dir / "deduplication_report.json"
        
        report_data = {
            "pipeline_run_id": run_id,
            "generated_at": datetime.now().isoformat(),
            "total_tables_processed": len(dedup_reports),
            "total_duplicates_removed": sum(r["duplicate_rows_removed"] for r in dedup_reports),
            "referential_integrity_summary": ri_counts,
            "deduplication_summary": dedup_reports
        }
        
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2)
            
        try:
            from src.audit import log_data_quality_results
            log_data_quality_results(run_id, dedup_reports)
        except Exception as audit_err:
            logger.warning(f"Could not log data quality results to database: {str(audit_err)}", run_id=run_id, task_name="bronze_to_silver")

        logger.info(
            f"Deduplication & Referential Integrity report saved to {report_file}",
            run_id=run_id,
            task_name="bronze_to_silver",
            target="deduplication_report.json",
            record_count=len(dedup_reports)
        )
    except Exception as e:
        logger.warning(
            f"Failed to save data quality report: {str(e)}",
            run_id=run_id,
            task_name="bronze_to_silver"
        )

    spark.stop()
    logger.info(
        "Bronze to Silver PySpark Job completed successfully!",
        run_id=run_id,
        task_name="bronze_to_silver"
    )


if __name__ == "__main__":
    main()
