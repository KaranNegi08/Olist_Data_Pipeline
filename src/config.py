"""
Reads pipeline.yml, table_schemas.yml, and environment variables from .env.
"""

import os
from pathlib import Path
import yaml
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

def load_yaml_config(file_path: Path) -> dict:
    """Reads a YAML configuration file."""
    if not file_path.exists():
        raise FileNotFoundError(f"Configuration file not found at: {file_path}")
    with file_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_pipeline_config() -> dict:
    """Returns merged pipeline configuration dictionary."""
    pipeline_cfg = load_yaml_config(BASE_DIR / "config" / "pipeline.yml")
    schemas_cfg = load_yaml_config(BASE_DIR / "config" / "table_schemas.yml")
    
    pg_host = os.getenv("POSTGRES_HOST", "localhost")
    pg_port = os.getenv("POSTGRES_PORT", "5432")
    pg_db = os.getenv("POSTGRES_DB", "olist_warehouse")
    
    # Resolve dynamic environment variables
    env_vars = {
        "POSTGRES_HOST": pg_host,
        "POSTGRES_PORT": pg_port,
        "POSTGRES_DB": pg_db,
        "POSTGRES_USER": os.getenv("POSTGRES_USER", "olist_admin"),
        "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", "olist_password_secure"),
        "POSTGRES_JDBC_URL": os.getenv("POSTGRES_JDBC_URL", f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_db}"),
        "SPARK_MASTER": os.getenv("SPARK_MASTER", "local[*]"),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
        "BASE_DIR": str(BASE_DIR)
    }
    
    return {
        "pipeline": pipeline_cfg.get("pipeline", {}),
        "storage": pipeline_cfg.get("storage", {}),
        "database": pipeline_cfg.get("database", {}),
        "quality_gate": pipeline_cfg.get("quality_gate", {}),
        "tables": schemas_cfg.get("tables", {}),
        "env": env_vars
    }
