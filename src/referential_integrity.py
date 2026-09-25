"""
Provides left-anti join utility functions for identifying missing parent records 
(Orders, Customers, Products, Sellers) and enforcing Referential Integrity.
"""

from typing import Dict, Tuple
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.logging_config import setup_logger

logger = setup_logger("referential_integrity")

def find_orphan_records(child_df: DataFrame, parent_df: DataFrame, join_key: str) -> DataFrame:
    """
    Uses left-anti join to identify child records missing a corresponding parent key.
    """
    return child_df.join(
        parent_df.select(join_key).distinct(),
        on=join_key,
        how="left_anti"
    )

def _quarantine_orphans(orphan_df: DataFrame, table_name: str, rejection_reason: str, quarantine_dir: Path, run_id: str):
    """Helper to write referential integrity orphan records to Quarantine Parquet storage."""
    if quarantine_dir is not None and orphan_df is not None:
        try:
            out_quarantine = quarantine_dir / table_name
            file_name = f"olist_{table_name}_dataset.csv"
            (
                orphan_df
                .withColumn("rejection_reason", F.lit(rejection_reason))
                .withColumn("source_file_name", F.lit(file_name))
                .withColumn("pipeline_run_id", F.lit(run_id))
                .withColumn("rejected_at", F.current_timestamp())
                .write.mode("append").parquet(str(out_quarantine))
            )
        except Exception as e:
            logger.warning(f"Failed to write RI quarantine records for {table_name}: {str(e)}", run_id=run_id, task_name="referential_integrity")

def validate_referential_integrity(
    dfs: Dict[str, DataFrame], 
    quarantine_dir: Path = None, 
    run_id: str = "N/A"
) -> Tuple[Dict[str, DataFrame], Dict[str, int]]:
    """
    Evaluates Referential Integrity rules across all datasets:
    1. Unknown Orders: order_items, order_payments, order_reviews -> orders (order_id)
    2. Unknown Customers: orders -> customers (customer_id)
    3. Unknown Products: order_items -> products (product_id)
    4. Unknown Sellers: order_items -> sellers (seller_id)

    Returns updated DataFrames dict (clean records only) and RI orphan counts summary dict.
    """
    ri_counts = {
        "unknown_orders": 0,
        "unknown_customers": 0,
        "unknown_products": 0,
        "unknown_sellers": 0
    }

    # 1. Unknown Customers (orders -> customers)
    if "orders" in dfs and "customers" in dfs:
        orphan_orders = find_orphan_records(dfs["orders"], dfs["customers"], "customer_id")
        count = orphan_orders.count()
        ri_counts["unknown_customers"] += count
        if count > 0:
            logger.warning(
                f"Referential Integrity: Found {count} orders with unknown customer_id",
                run_id=run_id,
                task_name="referential_integrity",
                target="orders",
                record_count=count
            )
            _quarantine_orphans(orphan_orders, "orders", "UNKNOWN_CUSTOMER_ID", quarantine_dir, run_id)
            dfs["orders"] = dfs["orders"].join(dfs["customers"].select("customer_id"), on="customer_id", how="left_semi")

    # 2. Unknown Orders (order_items -> orders, order_payments -> orders, order_reviews -> orders)
    if "orders" in dfs:
        parent_orders = dfs["orders"]

        # order_items
        if "order_items" in dfs:
            orphan_items = find_orphan_records(dfs["order_items"], parent_orders, "order_id")
            count = orphan_items.count()
            ri_counts["unknown_orders"] += count
            if count > 0:
                logger.warning(
                    f"Referential Integrity: Found {count} order_items with unknown order_id",
                    run_id=run_id,
                    task_name="referential_integrity",
                    target="order_items",
                    record_count=count
                )
                _quarantine_orphans(orphan_items, "order_items", "UNKNOWN_ORDER_ID", quarantine_dir, run_id)
                dfs["order_items"] = dfs["order_items"].join(parent_orders.select("order_id"), on="order_id", how="left_semi")

        # order_payments
        if "order_payments" in dfs:
            orphan_payments = find_orphan_records(dfs["order_payments"], parent_orders, "order_id")
            count = orphan_payments.count()
            ri_counts["unknown_orders"] += count
            if count > 0:
                logger.warning(
                    f"Referential Integrity: Found {count} order_payments with unknown order_id",
                    run_id=run_id,
                    task_name="referential_integrity",
                    target="order_payments",
                    record_count=count
                )
                _quarantine_orphans(orphan_payments, "order_payments", "UNKNOWN_ORDER_ID", quarantine_dir, run_id)
                dfs["order_payments"] = dfs["order_payments"].join(parent_orders.select("order_id"), on="order_id", how="left_semi")

        # order_reviews
        if "order_reviews" in dfs:
            orphan_reviews = find_orphan_records(dfs["order_reviews"], parent_orders, "order_id")
            count = orphan_reviews.count()
            ri_counts["unknown_orders"] += count
            if count > 0:
                logger.warning(
                    f"Referential Integrity: Found {count} order_reviews with unknown order_id",
                    run_id=run_id,
                    task_name="referential_integrity",
                    target="order_reviews",
                    record_count=count
                )
                _quarantine_orphans(orphan_reviews, "order_reviews", "UNKNOWN_ORDER_ID", quarantine_dir, run_id)
                dfs["order_reviews"] = dfs["order_reviews"].join(parent_orders.select("order_id"), on="order_id", how="left_semi")

    # 3. Unknown Products (order_items -> products)
    if "order_items" in dfs and "products" in dfs:
        orphan_prods = find_orphan_records(dfs["order_items"], dfs["products"], "product_id")
        count = orphan_prods.count()
        ri_counts["unknown_products"] += count
        if count > 0:
            logger.warning(
                f"Referential Integrity: Found {count} order_items with unknown product_id",
                run_id=run_id,
                task_name="referential_integrity",
                target="order_items",
                record_count=count
            )
            _quarantine_orphans(orphan_prods, "order_items", "UNKNOWN_PRODUCT_ID", quarantine_dir, run_id)
            dfs["order_items"] = dfs["order_items"].join(dfs["products"].select("product_id"), on="product_id", how="left_semi")

    # 4. Unknown Sellers (order_items -> sellers)
    if "order_items" in dfs and "sellers" in dfs:
        orphan_sellers = find_orphan_records(dfs["order_items"], dfs["sellers"], "seller_id")
        count = orphan_sellers.count()
        ri_counts["unknown_sellers"] += count
        if count > 0:
            logger.warning(
                f"Referential Integrity: Found {count} order_items with unknown seller_id",
                run_id=run_id,
                task_name="referential_integrity",
                target="order_items",
                record_count=count
            )
            _quarantine_orphans(orphan_sellers, "order_items", "UNKNOWN_SELLER_ID", quarantine_dir, run_id)
            dfs["order_items"] = dfs["order_items"].join(dfs["sellers"].select("seller_id"), on="seller_id", how="left_semi")

    return dfs, ri_counts
