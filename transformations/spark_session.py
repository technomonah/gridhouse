"""SparkSession builder for grindhouse Silver transformations.

Creates a local-mode Spark session pre-configured with:
- Iceberg table format (iceberg-spark-runtime)
- Nessie catalog via Iceberg REST
- S3A connector pointing at MinIO

All configuration is read from environment variables — no hardcoded values.
Call get_spark() once per process; SparkSession is a singleton.

Usage:
    from transformations.config import get_spark
    spark = get_spark()
    spark.sql("SHOW NAMESPACES IN nessie").show()
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------

NESSIE_URI = os.environ.get("NESSIE_URI", "http://localhost:19120/iceberg/")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

# ---------------------------------------------------------------------------
# JAR paths — pre-downloaded to jars/ directory, mounted at /opt/spark/jars-extra
# ---------------------------------------------------------------------------
# Image: apache/spark-py:latest = Spark 3.4.x (Scala 2.12, OpenJDK 17).
# JARs are pre-downloaded rather than resolved via Maven at runtime to avoid
# Ivy2 permission issues inside the apache/spark-py container.
#
# Versions:
#   iceberg-spark-runtime-3.4_2.12:1.5.2  — Iceberg + Spark integration
#   iceberg-aws-bundle:1.5.2              — Iceberg S3FileIO + AWS SDK v2 (required for S3)
#   nessie-spark-extensions-3.4_2.12:0.106.0 — last version with Spark 3.4 support
#   hadoop-aws:3.3.4                      — S3A connector for Hadoop filesystem API
#
# Note: iceberg-aws-bundle replaces aws-java-sdk-bundle — it includes AWS SDK v2
# which is required by Iceberg's S3FileIO implementation.
_JARS_DIR = "/opt/spark/jars-extra"
_SPARK_JARS = ",".join([
    f"{_JARS_DIR}/iceberg-spark-runtime-3.4_2.12-1.5.2.jar",
    f"{_JARS_DIR}/iceberg-aws-bundle-1.5.2.jar",
    f"{_JARS_DIR}/nessie-spark-extensions-3.4_2.12-0.106.0.jar",
    f"{_JARS_DIR}/hadoop-aws-3.3.4.jar",
])


def get_spark(app_name: str = "grindhouse") -> SparkSession:
    """Build and return a SparkSession configured for Iceberg + Nessie + MinIO.

    SparkSession is a singleton — calling this multiple times returns the same
    session. The session runs in local[*] mode (all available cores on one node).

    Args:
        app_name: Application name shown in the Spark UI.

    Returns:
        Ready-to-use SparkSession with Iceberg, Nessie catalog, and S3A configured.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Iceberg + Nessie SQL extensions — required for CREATE TABLE USING iceberg
        # and Nessie branch/tag DDL in Spark SQL.
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ",org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        # Register 'nessie' as an Iceberg catalog backed by the Nessie REST API.
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.type", "rest")
        .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .config("spark.sql.catalog.nessie.warehouse", ICEBERG_WAREHOUSE)
        # S3FileIO config — used by Iceberg (via iceberg-aws-bundle / AWS SDK v2).
        # path-style-access is required for MinIO.
        .config("spark.sql.catalog.nessie.s3.endpoint", S3_ENDPOINT)
        .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
        .config("spark.sql.catalog.nessie.s3.access-key-id", S3_ACCESS_KEY)
        .config("spark.sql.catalog.nessie.s3.secret-access-key", S3_SECRET_KEY)
        # S3A connector — also configured for Hadoop filesystem API compatibility.
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Disable AWS credential chain — use explicit keys only (no EC2 metadata lookup).
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Pre-downloaded JARs — avoids Ivy2 Maven resolver permission issues at runtime.
        .config("spark.jars", _SPARK_JARS)
        .getOrCreate()
    )
