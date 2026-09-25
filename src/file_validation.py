"""
Pre-flight verification of source CSV files before PySpark processing.
"""

import csv
from pathlib import Path
from typing import List, Tuple
from src.logging_config import setup_logger

logger = setup_logger("file_validation")

def read_header(path: Path) -> List[str]:
    """Reads the CSV header line safely, stripping UTF-8 BOM marks if present."""
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source)
        header = next(reader, [])
        return [col.replace('\ufeff', '').strip() for col in header]


def validate_csv_file(path: Path, expected_columns: List[str] = None, run_id: str = "N/A") -> Tuple[bool, str]:
    """
    Validates CSV file existence, non-emptiness, and optional header presence.
    """
    target_name = path.name
    if not path.exists():
        msg = f"File does not exist: {target_name}"
        logger.warning(msg, target=target_name, task_name="file_validation", run_id=run_id)
        return False, msg

    if path.stat().st_size == 0:
        msg = f"File is empty (0 bytes): {target_name}"
        logger.warning(msg, target=target_name, task_name="file_validation", run_id=run_id)
        return False, msg

    try:
        header = read_header(path)
        if not header:
            msg = f"File contains no header row: {target_name}"
            logger.warning(msg, target=target_name, task_name="file_validation", run_id=run_id)
            return False, msg

        if expected_columns:
            missing = set(expected_columns) - set(header)
            if missing:
                msg = f"Missing expected columns in {target_name}: {missing}"
                logger.warning(msg, target=target_name, task_name="file_validation", run_id=run_id)
                return False, msg
    except Exception as e:
        msg = f"Failed to read CSV header from {target_name}: {str(e)}"
        logger.error(msg, target=target_name, task_name="file_validation", run_id=run_id, exc_info=True)
        return False, msg

    logger.info(f"File validation PASSED: {target_name}", target=target_name, task_name="file_validation", run_id=run_id)
    return True, "VALID"

