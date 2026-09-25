"""
Database initialization script: Executes DDL files (01 to 05) to set up schemas, audit tables,
warehouse dimensions, facts, indexes, and seeds dim_date.
"""

import os
import sys
import psycopg2
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.config import get_pipeline_config
from src.logging_config import setup_logger

logger = setup_logger("init_db")

def initialize_database(run_id: str = "N/A") -> bool:
    """Connects to PostgreSQL and executes all DDL files in sql/ directory sequentially."""
    config = get_pipeline_config()
    env = config["env"]
    
    try:
        conn = psycopg2.connect(
            host=env.get("POSTGRES_HOST", "localhost"),
            port=int(env.get("POSTGRES_PORT", 5432)),
            dbname=env.get("POSTGRES_DB", "olist_warehouse"),
            user=env.get("POSTGRES_USER", "olist_admin"),
            password=env.get("POSTGRES_PASSWORD", "olist_password_secure")
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        sql_files = [
            "01_create_schemas.sql",
            "02_create_audit_tables.sql",
            "03_create_dimensions.sql",
            "04_create_facts.sql",
            "05_load_dimensions.sql",
            "08_reporting_views.sql"
        ]
        
        sql_dir = BASE_DIR / "sql"
        for sql_file in sql_files:
            file_path = sql_dir / sql_file
            if file_path.exists():
                logger.info(f"Executing DDL script: {sql_file}", run_id=run_id, task_name="init_db", target=sql_file)
                sql_content = file_path.read_text(encoding="utf-8")
                cursor.execute(sql_content)
                logger.info(f"DDL script executed successfully: {sql_file}", run_id=run_id, task_name="init_db", target=sql_file)
            else:
                logger.warning(f"SQL file not found: {sql_file}", run_id=run_id, task_name="init_db")
                
        cursor.close()
        conn.close()
        logger.info("PostgreSQL Database initialized successfully with all schemas, tables, and seed dimensions!", run_id=run_id, task_name="init_db")
        return True
        
    except Exception as e:
        logger.error(f"Failed to initialize PostgreSQL database: {str(e)}", run_id=run_id, task_name="init_db", exc_info=True)
        return False

if __name__ == "__main__":
    initialize_database()
