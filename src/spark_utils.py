"""
Centralized SparkSession creation and Java environment configuration for PySpark jobs.
"""

import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

def setup_java_home():
    """Ensures JAVA_HOME environment variable is configured for PySpark on Windows."""
    if "JAVA_HOME" not in os.environ or not os.path.exists(os.environ["JAVA_HOME"]):
        if os.path.exists(r"C:\java\jdk"):
            os.environ["JAVA_HOME"] = r"C:\java\jdk"
            os.environ["PATH"] = r"C:\java\jdk\bin;" + os.environ.get("PATH", "")

def create_spark_session(app_name: str = "olist-spark-job") -> SparkSession:
    """Creates and configures a unified SparkSession with Java 17/21 compatibility and dynamic master resolution."""
    setup_java_home()

    java_opens = (
        "--add-opens=java.base/java.lang=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
        "--add-opens=java.base/java.io=ALL-UNNAMED "
        "--add-opens=java.base/java.net=ALL-UNNAMED "
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
        "--add-opens=java.base/java.util=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .config("spark.driver.extraJavaOptions", java_opens)
        .config("spark.executor.extraJavaOptions", java_opens)
        .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .config("spark.hadoop.fs.permissions.umask-mode", "000")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )

    if not os.environ.get("SPARK_MASTER_URL"):
        builder = builder.master("local[*]")

    return builder.getOrCreate()
