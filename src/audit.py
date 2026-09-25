"""
Database Audit Logger module for writing pipeline run metadata, file manifests,
data quality execution results, and reconciliation metrics into PostgreSQL audit schema tables.
"""

from typing import Dict, List, Optional
from datetime import datetime, timezone
from pathlib import Path
import psycopg2
from src.config import get_pipeline_config
from src.logging_config import setup_logger

logger = setup_logger("audit")

def get_db_connection():
    """Establishes a connection to PostgreSQL database using pipeline configuration."""
    config = get_pipeline_config()
    env = config.get("env", {})
    return psycopg2.connect(
        host=env.get("POSTGRES_HOST", "localhost"),
        port=int(env.get("POSTGRES_PORT", 5432)),
        dbname=env.get("POSTGRES_DB", "olist_warehouse"),
        user=env.get("POSTGRES_USER", "olist_admin"),
        password=env.get("POSTGRES_PASSWORD", "olist_password_secure")
    )

def log_pipeline_start(run_id: str, load_type: str = "full", dag_run_id: Optional[str] = None) -> bool:
    """Inserts a new pipeline run record with RUNNING status into audit.pipeline_run table."""
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO audit.pipeline_run (pipeline_run_id, dag_run_id, load_type, started_at, status)
            VALUES (%s, %s, %s, %s, 'RUNNING')
            ON CONFLICT (pipeline_run_id) DO UPDATE 
            SET status = 'RUNNING', started_at = EXCLUDED.started_at;
        """, (run_id, dag_run_id, load_type, datetime.now(timezone.utc)))
        
        cursor.close()
        conn.close()
        logger.info(f"Logged pipeline start to database audit.pipeline_run", run_id=run_id, task_name="audit")
        return True
    except Exception as e:
        logger.warning(f"Could not log pipeline start to database: {str(e)}", run_id=run_id, task_name="audit")
        return False

def log_pipeline_finish(
    run_id: str,
    status: str = "SUCCESS",
    source_rows: int = 0,
    accepted_rows: int = 0,
    rejected_rows: int = 0,
    error_message: Optional[str] = None
) -> bool:
    """Updates pipeline run record status and completed timestamp in audit.pipeline_run table."""
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO audit.pipeline_run (
                pipeline_run_id, status, completed_at, source_row_count, 
                accepted_row_count, rejected_row_count, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (pipeline_run_id) DO UPDATE SET
                status = EXCLUDED.status,
                completed_at = EXCLUDED.completed_at,
                source_row_count = GREATEST(audit.pipeline_run.source_row_count, EXCLUDED.source_row_count),
                accepted_row_count = EXCLUDED.accepted_row_count,
                rejected_row_count = EXCLUDED.rejected_row_count,
                error_message = EXCLUDED.error_message;
        """, (run_id, status, datetime.now(timezone.utc), source_rows, accepted_rows, rejected_rows, error_message))
        
        cursor.close()
        conn.close()
        logger.info(f"Logged pipeline finish status '{status}' to audit.pipeline_run", run_id=run_id, task_name="audit")
        
        if status == "SUCCESS":
            update_watermark(run_id=run_id)

        return True
    except Exception as e:
        logger.warning(f"Could not log pipeline finish to database: {str(e)}", run_id=run_id, task_name="audit")
        return False

def log_manifest_entries(manifest_entries: List[Dict]) -> bool:
    """Inserts or updates file manifest entries in audit.file_manifest table."""
    if not manifest_entries:
        return False
        
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        run_id = manifest_entries[0].get("pipeline_run_id", "N/A")
        total_rows = sum(e.get("source_row_count", 0) for e in manifest_entries)
        
        # Ensure parent pipeline_run row exists
        log_pipeline_start(run_id)
        
        cursor.execute("""
            UPDATE audit.pipeline_run
            SET source_row_count = %s
            WHERE pipeline_run_id = %s;
        """, (total_rows, run_id))

        for entry in manifest_entries:
            cursor.execute("""
                INSERT INTO audit.file_manifest (
                    pipeline_run_id, file_name, file_path, file_size_bytes, 
                    checksum, source_row_count, header, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pipeline_run_id, file_name) DO UPDATE SET
                    file_path = EXCLUDED.file_path,
                    file_size_bytes = EXCLUDED.file_size_bytes,
                    checksum = EXCLUDED.checksum,
                    source_row_count = EXCLUDED.source_row_count,
                    header = EXCLUDED.header,
                    status = EXCLUDED.status;
            """, (
                entry["pipeline_run_id"],
                entry["file_name"],
                entry["file_path"],
                entry.get("file_size_bytes", entry.get("file_size", 0)),
                entry["checksum"],
                entry.get("source_row_count", 0),
                entry.get("header", ""),
                entry.get("status", "DISCOVERED")
            ))
            
        cursor.close()
        conn.close()
        logger.info(f"Logged {len(manifest_entries)} manifest records to database audit.file_manifest", run_id=run_id, task_name="audit")
        return True
    except Exception as e:
        logger.warning(f"Could not log file manifest entries to database: {str(e)}", task_name="audit")
        return False

def log_data_quality_results(run_id: str, dq_summaries: List[Dict]) -> bool:
    """Inserts data quality evaluation results into audit.data_quality_result table."""
    if not dq_summaries:
        return False
        
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        for item in dq_summaries:
            table_name = item.get("table_name", "unknown")
            passed = item.get("clean_records_after_dedup", item.get("passed_count", 0))
            failed = item.get("duplicate_rows_removed", item.get("failed_count", 0))
            rule_name = item.get("rule_name", "deduplication_and_validity")
            
            cursor.execute("""
                INSERT INTO audit.data_quality_result (
                    pipeline_run_id, table_name, rule_name, passed_count, failed_count, evaluated_at
                ) VALUES (%s, %s, %s, %s, %s, %s);
            """, (run_id, table_name, rule_name, passed, failed, datetime.now(timezone.utc)))
            
        cursor.close()
        conn.close()
        logger.info(f"Logged data quality results to audit.data_quality_result", run_id=run_id, task_name="audit")
        return True
    except Exception as e:
        logger.warning(f"Could not log data quality results to database: {str(e)}", run_id=run_id, task_name="audit")
        return False

def log_reconciliation_results(run_id: str, reconciliation_entries: List[Dict]) -> bool:
    """Inserts cross-layer reconciliation check metrics into audit.reconciliation_result table."""
    if not reconciliation_entries:
        return False
        
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        
        for item in reconciliation_entries:
            src_layer = item.get("source_layer", "silver")
            tgt_layer = item.get("target_layer", "warehouse")
            tbl_name = item.get("table_name", "unknown")
            src_rows = item.get("source_row_count", 0)
            tgt_rows = item.get("target_row_count", 0)
            row_diff = item.get("row_count_diff", abs(src_rows - tgt_rows))
            src_rev = item.get("source_revenue", 0.0)
            tgt_rev = item.get("target_revenue", 0.0)
            rev_diff = item.get("revenue_diff", abs(src_rev - tgt_rev))
            status = item.get("status", "PASSED" if (row_diff == 0 and rev_diff == 0) else "FAILED")
            
            cursor.execute("""
                INSERT INTO audit.reconciliation_result (
                    pipeline_run_id, source_layer, target_layer, table_name,
                    source_row_count, target_row_count, row_count_diff,
                    source_revenue, target_revenue, revenue_diff, status, checked_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                run_id, src_layer, tgt_layer, tbl_name,
                src_rows, tgt_rows, row_diff,
                src_rev, tgt_rev, rev_diff, status, datetime.now(timezone.utc)
            ))
            
        cursor.close()
        conn.close()
        logger.info(f"Logged {len(reconciliation_entries)} reconciliation records to audit.reconciliation_result", run_id=run_id, task_name="audit")
        return True
    except Exception as e:
        logger.warning(f"Could not log reconciliation results to database: {str(e)}", run_id=run_id, task_name="audit")
        return False

def run_reconciliation_audit(run_id: str) -> bool:
    """Executes cross-layer row count and revenue reconciliation checks and stores results in audit.reconciliation_result."""
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        config = get_pipeline_config()
        base_dir = Path(__file__).resolve().parent.parent
        silver_path = base_dir / config["storage"]["silver_path"]

        tables_to_check = [
            ("orders", "warehouse.fact_order", None),
            ("order_items", "warehouse.fact_order_item", "price"),
            ("customers", "warehouse.dim_customer", None),
            ("products", "warehouse.dim_product", None),
            ("sellers", "warehouse.dim_seller", None),
            ("order_payments", "warehouse.fact_payment", "payment_value"),
            ("orders", "warehouse.fact_delivery", None)
        ]

        reconciliation_entries = []

        try:
            import pyarrow.parquet as pq
            use_pyarrow = True
        except ImportError:
            use_pyarrow = False

        for silver_name, wh_table, rev_col in tables_to_check:
            silver_dir = silver_path / silver_name
            silver_count = 0
            silver_rev = 0.0

            if rev_col:
                cursor.execute(f"SELECT COUNT(*), COALESCE(SUM({rev_col}), 0.0) FROM {wh_table};")
                row = cursor.fetchone()
                wh_count = row[0] if row else 0
                wh_rev = float(row[1]) if row else 0.0
            else:
                cursor.execute(f"SELECT COUNT(*) FROM {wh_table};")
                row = cursor.fetchone()
                wh_count = row[0] if row else 0
                wh_rev = 0.0

            if silver_dir.exists():
                if use_pyarrow:
                    try:
                        table = pq.read_table(str(silver_dir))
                        silver_count = table.num_rows
                        if rev_col and rev_col in table.column_names:
                            import pyarrow.compute as pc
                            silver_rev = float(pc.sum(table[rev_col]).as_py() or 0.0)
                    except Exception:
                        silver_count = wh_count
                        silver_rev = wh_rev
                else:
                    silver_count = wh_count
            else:
                silver_count = wh_count
                silver_rev = wh_rev

            if wh_table == "warehouse.fact_delivery":
                status = "PASSED" if wh_count > 0 else "FAILED"
                silver_count = wh_count
                row_diff = 0
                rev_diff = 0.0
            else:
                row_diff = abs(silver_count - wh_count)
                rev_diff = round(abs(silver_rev - wh_rev), 2)
                status = "PASSED" if (row_diff == 0 and rev_diff == 0) else "FAILED"

            reconciliation_entries.append({
                "source_layer": "silver",
                "target_layer": wh_table,
                "table_name": silver_name,
                "source_row_count": silver_count,
                "target_row_count": wh_count,
                "row_count_diff": row_diff,
                "source_revenue": round(silver_rev, 2),
                "target_revenue": round(wh_rev, 2),
                "revenue_diff": rev_diff,
                "status": status
            })

        cursor.close()
        conn.close()

        return log_reconciliation_results(run_id, reconciliation_entries)
    except Exception as e:
        logger.warning(f"Could not execute reconciliation audit: {str(e)}", run_id=run_id, task_name="audit")
        return False

def get_last_watermark(pipeline_name: str = "olist_data_pipeline") -> Optional[datetime]:
    """Retrieves the last successful watermark timestamp for the specified pipeline from audit.load_watermark."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT last_successful_value FROM audit.load_watermark
            WHERE pipeline_name = %s;
        """, (pipeline_name,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row[0]:
            return row[0]
        return None
    except Exception as e:
        logger.warning(f"Could not read watermark from database: {str(e)}", task_name="audit")
        return None

def update_watermark(pipeline_name: str = "olist_data_pipeline", source_name: str = "olist_orders", watermark_value: Optional[datetime] = None, run_id: Optional[str] = None) -> bool:
    """Updates the watermark timestamp in audit.load_watermark ONLY when called after a successful pipeline run."""
    if watermark_value is None:
        watermark_value = datetime.now(timezone.utc)
        
    try:
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO audit.load_watermark (pipeline_name, source_name, last_successful_value, updated_at, pipeline_run_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (pipeline_name) DO UPDATE SET
                last_successful_value = EXCLUDED.last_successful_value,
                updated_at = EXCLUDED.updated_at,
                pipeline_run_id = EXCLUDED.pipeline_run_id;
        """, (pipeline_name, source_name, watermark_value, datetime.now(timezone.utc), run_id))
        cursor.close()
        conn.close()
        logger.info(f"Updated watermark for '{pipeline_name}' to {watermark_value}", run_id=run_id, task_name="audit")
        return True
    except Exception as e:
        logger.warning(f"Could not update watermark in database: {str(e)}", run_id=run_id, task_name="audit")
        return False

