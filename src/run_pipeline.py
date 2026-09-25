"""
End-to-End Execution Orchestrator for Olist Data Engineering Pipeline.
Triggers pre-flight manifest discovery, PySpark Bronze-to-Silver ETL,
PySpark Silver-to-Warehouse transformations, and verification checks.
"""

import sys
import subprocess
from pathlib import Path
from uuid import uuid4
import time

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from src.logging_config import setup_logger
from src.manifest import discover_and_build_manifest, copy_to_bronze
from src.audit import log_pipeline_start, log_pipeline_finish, run_reconciliation_audit

logger = setup_logger("run_pipeline")

def run_step(step_name: str, command: list, run_id: str = "N/A") -> bool:
    """Executes a subprocess pipeline command and logs ."""
    logger.info(f"STARTING STEP: {step_name}", run_id=run_id, task_name=step_name)
    
    start_time = time.time()
    res = subprocess.run(command, capture_output=True, text=True)
    duration = time.time() - start_time
    
    if res.stdout:
        print(res.stdout)
        
    if res.returncode != 0:
        logger.error(f"STEP FAILED: {step_name} (Duration: {duration:.2f}s)", run_id=run_id, task_name=step_name, exc_info=False)
        if res.stderr:
            print(res.stderr)
        return False
        
    logger.info(f"STEP PASSED: {step_name} (Duration: {duration:.2f}s)", run_id=run_id, task_name=step_name)
    return True

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Olist End-to-End Pipeline Runner")
    parser.add_argument("--load-type", type=str, default="full", choices=["full", "incremental"], help="Load type: full or incremental")
    args = parser.parse_args()

    run_id = str(uuid4())
    logger.info(f"Starting Olist End-to-End Pipeline Execution (Load Type: {args.load_type})", run_id=run_id, task_name="orchestrator")
    log_pipeline_start(run_id, load_type=args.load_type)
    
    python_exe = sys.executable
    bronze_dir = BASE_DIR / "data_lake" / "bronze" / "olist"
    raw_dir = BASE_DIR / "data_lake" / "raw"

    # Step 1: Pre-flight Ingestion, Manifest & Checksums
    logger.info("Running Pre-flight Source Discovery and SHA256 Checksums...", run_id=run_id, task_name="preflight_manifest")
    if raw_dir.exists():
        for src_file in raw_dir.rglob("*.csv"):
            copy_to_bronze(src_file, bronze_dir / src_file.name)

    manifest = discover_and_build_manifest(bronze_dir, run_id)
    logger.info("Manifest Generated successfully.", run_id=run_id, task_name="preflight_manifest", target="manifest", record_count=len(manifest))

    # Step 2: PySpark Bronze to Silver (DQ, Quarantine & Parquet)
    step2_cmd = [python_exe, str(BASE_DIR / "spark_jobs" / "bronze_to_silver.py"), "--run-id", run_id, "--load-type", args.load_type]
    if not run_step("Bronze_to_Silver_ETL", step2_cmd, run_id=run_id):
        log_pipeline_finish(run_id, status="FAILED", error_message="Bronze_to_Silver_ETL step failed")
        sys.exit(1)

    # Step 3: PySpark Silver to Warehouse Dimensions & Facts
    step3_cmd = [python_exe, str(BASE_DIR / "spark_jobs" / "silver_to_warehouse.py"), "--run-id", run_id, "--load-type", args.load_type]
    if not run_step("Silver_to_Warehouse_Transformations", step3_cmd, run_id=run_id):
        log_pipeline_finish(run_id, status="FAILED", error_message="Silver_to_Warehouse_Transformations step failed")
        sys.exit(1)

    # Step 4: Cross-Layer Reconciliation & Audit Logging
    logger.info("Running Cross-Layer Reconciliation Audit...", run_id=run_id, task_name="reconciliation_audit")
    run_reconciliation_audit(run_id)

    # Step 5: Unit Test Suite Verification
    step4_cmd = [python_exe, "-m", "pytest", str(BASE_DIR / "tests")]
    if not run_step("Pytest_Verification_Suite", step4_cmd, run_id=run_id):
        log_pipeline_finish(run_id, status="FAILED", error_message="Pytest_Verification_Suite step failed")
        sys.exit(1)

    log_pipeline_finish(run_id, status="SUCCESS")
    logger.info("END-TO-END PIPELINE COMPLETED SUCCESSFULLY!", run_id=run_id, task_name="orchestrator")



if __name__ == "__main__":
    main()
