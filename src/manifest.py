"""
Generates file manifest records and SHA256 checksums for source CSV files.
"""

from pathlib import Path
from hashlib import sha256
from uuid import uuid4
from datetime import datetime, timezone
import csv
import json
import shutil
from typing import Dict, List, Optional
from src.logging_config import setup_logger
from src.file_validation import read_header

logger = setup_logger("manifest")

def calculate_checksum(path: Path) -> str:
    """Calculates SHA256 checksum of a file in 1MB chunks."""
    digest = sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def copy_to_bronze(source_path: Path, target_path: Path, checksum: Optional[str] = None) -> bool:
    """
    Copies source file to Bronze target destination idempotently.
    Skipping if target file already exists and has identical SHA256 checksum.

    Returns True if file was copied, False if skipped.
    """
    if checksum is None:
        checksum = calculate_checksum(source_path)

    if target_path.exists() and calculate_checksum(target_path) == checksum:
        logger.info(
            f"Bronze copy already present and identical ({target_path.name}), skipping copy.",
            task_name="bronze_ingest",
            target=target_path.name
        )
        return False

    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    logger.info(
        f"Copied {source_path.name} -> {target_path.parent}",
        task_name="bronze_ingest",
        target=target_path.name
    )
    return True

def count_file_rows(path: Path) -> int:
    """Counts total data rows in a CSV file (excluding header)."""
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        count = sum(1 for _ in f)
    return max(0, count - 1)

def get_dataset_name(file_name: str) -> str:
    """Dynamically resolves dataset key from file_name using configuration."""
    try:
        from src.config import get_pipeline_config
        cfg = get_pipeline_config()
        tables = cfg.get("tables", {})
        for dataset_key, info in tables.items():
            if info.get("file_name") == file_name:
                return dataset_key
    except Exception:
        pass
    return file_name.replace("olist_", "").replace("_dataset.csv", "").replace(".csv", "")


def create_manifest_entry(path: Path, run_id: str, execution_date: Optional[str] = None) -> Dict:
    """Creates a standardized manifest metadata dictionary for a source file."""
    header = read_header(path)
    checksum = calculate_checksum(path)
    row_count = count_file_rows(path)
    now_dt = datetime.now(timezone.utc)
    discovered_at = now_dt.isoformat()
    
    if execution_date:
        date_str = str(execution_date)[:10]
    else:
        date_str = now_dt.strftime("%Y-%m-%d")
        
    file_size = path.stat().st_size
    return {
        "pipeline_run_id": run_id,
        "dataset_name": get_dataset_name(path.name),
        "file_name": path.name,
        "file_path": str(path.resolve()),
        "file_size_bytes": file_size,
        "file_size": file_size,
        "checksum": checksum,
        "source_row_count": row_count,
        "record_count": row_count,
        "header": ",".join(header),
        "discovered_date": date_str,
        "discovered_timestamp": discovered_at,
        "status": "DISCOVERED",
        "processing_status": "DISCOVERED"
    }

def save_manifest_report(manifest_entries: List[Dict], output_path: Path = None):
    """Saves manifest records array to a JSON report file and writes to database audit tables."""
    if output_path is None:
        output_path = Path(__file__).resolve().parent.parent / "reports" / "data_quality" / "manifest.json"
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(manifest_entries, f, indent=2)

    try:
        from src.audit import log_manifest_entries
        log_manifest_entries(manifest_entries)
    except Exception as e:
        logger.warning(f"Failed to log manifest entries to database audit tables: {str(e)}", task_name="manifest")

    return output_path

def discover_and_build_manifest(
    bronze_dir: Path,
    run_id: str = None,
    save_report: bool = True,
    source_dir: Optional[Path] = None,
    execution_date: Optional[str] = None
) -> List[Dict]:
    """Scans bronze directory for CSV files, computes SHA256 checksums, and saves manifest.json."""
    if run_id is None:
        run_id = str(uuid4())
        
    if source_dir and source_dir.exists():
        logger.info(f"Ingesting raw files from source directory: {source_dir}", run_id=run_id, task_name="bronze_ingest")
        for src_file in source_dir.rglob("*.csv"):
            target_file = bronze_dir / src_file.name
            copy_to_bronze(src_file, target_file)

    logger.info(f"Discovering source CSV files in: {bronze_dir}", run_id=run_id, task_name="manifest")
    manifest_entries = []
    
    if not bronze_dir.exists():
        logger.warning(f"Bronze directory does not exist yet: {bronze_dir}", run_id=run_id, task_name="manifest")
        return manifest_entries
        
    csv_files = list(bronze_dir.rglob("*.csv"))
    logger.info(f"Found {len(csv_files)} CSV files in bronze layer.", run_id=run_id, task_name="manifest", record_count=len(csv_files))
    
    for file_path in csv_files:
        try:
            entry = create_manifest_entry(file_path, run_id, execution_date=execution_date)
            manifest_entries.append(entry)
            logger.info(
                f"Manifest created for {file_path.name} | Checksum: {entry['checksum'][:10]}...",
                run_id=run_id,
                task_name="manifest",
                target=file_path.name,
                record_count=entry['source_row_count']
            )
        except Exception as e:
            logger.error(
                f"Failed to process manifest for {file_path.name}: {str(e)}",
                run_id=run_id,
                task_name="manifest",
                target=file_path.name,
                exc_info=True
            )
            
    if save_report and manifest_entries:
        report_file = save_manifest_report(manifest_entries)
        logger.info(
            f"Manifest report saved to {report_file}",
            run_id=run_id,
            task_name="manifest",
            target="manifest.json",
            record_count=len(manifest_entries)
        )

    return manifest_entries

