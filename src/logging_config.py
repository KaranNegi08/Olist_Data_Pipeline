"""
Provides standardized, structured logging across all Python modules and Spark jobs.
Log Format:
Timestamp | Log level | Pipeline run ID | Task name | File or table name | Record count | Message
"""

import logging
import sys
from pathlib import Path

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | RunID: %(run_id)s | Task: %(task_name)s | Target: %(target)s | Records: %(record_count)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class PipelineContextFilter(logging.Filter):
    """Injects default context fields (run_id, task_name, target, record_count) if missing."""
    def __init__(self, task_name: str = "olist_pipeline", run_id: str = "N/A"):
        super().__init__()
        self.default_task_name = task_name
        self.default_run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.run_id = getattr(record, "run_id", None) or self.default_run_id
        record.task_name = getattr(record, "task_name", None) or (record.name if record.name != "root" else self.default_task_name)
        record.target = getattr(record, "target", None) or getattr(record, "file_or_table", "N/A")
        if getattr(record, "record_count", None) is None:
            record.record_count = "N/A"
        return True


class PipelineLoggerAdapter(logging.LoggerAdapter):
    """LoggerAdapter that automatically captures pipeline context kwargs into extra dict."""
    def __init__(self, logger: logging.Logger, task_name: str = "olist_pipeline", run_id: str = "N/A"):
        super().__init__(logger, {"task_name": task_name, "run_id": run_id, "target": "N/A", "record_count": "N/A"})
        self.task_name = task_name
        self.run_id = run_id

    def process(self, msg, kwargs):
        extra = self.extra.copy()
        if "extra" in kwargs and kwargs["extra"]:
            extra.update(kwargs.pop("extra"))

        for key in ("run_id", "task_name", "target", "record_count", "file_or_table"):
            if key in kwargs:
                val = kwargs.pop(key)
                extra["target" if key == "file_or_table" else key] = val

        kwargs["extra"] = extra
        return msg, kwargs


def setup_logger(
    name: str = "olist_pipeline",
    log_level: str = "INFO",
    run_id: str = "N/A",
    log_to_file: bool = True
) -> PipelineLoggerAdapter:
    """Configures and returns a structured logger adapter printing to stdout and optional log file."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    context_filter = PipelineContextFilter(task_name=name, run_id=run_id)

    if not any(isinstance(f, PipelineContextFilter) for f in logger.filters):
        logger.addFilter(context_filter)

    if not logger.handlers:
        formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        stream_handler.addFilter(context_filter)
        logger.addHandler(stream_handler)

        if log_to_file:
            try:
                logs_dir = Path(__file__).resolve().parent.parent / "logs"
                logs_dir.mkdir(parents=True, exist_ok=True)
                file_handler = logging.FileHandler(logs_dir / "pipeline.log", encoding="utf-8")
                file_handler.setFormatter(formatter)
                file_handler.addFilter(context_filter)
                logger.addHandler(file_handler)
            except Exception:
                pass

    return PipelineLoggerAdapter(logger, task_name=name, run_id=run_id)


