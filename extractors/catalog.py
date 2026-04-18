"""Iceberg catalog connection helper.

Provides a single entry point for loading the Nessie REST catalog.
All extractors and scripts import get_catalog() from here instead of
duplicating connection config.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.table import Table

# Load .env from project root if running outside Docker
_env_path = Path(__file__).parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _key, _, _value = _line.partition("=")
            os.environ.setdefault(_key.strip(), _value.strip())

NESSIE_BRANCH = os.environ.get("NESSIE_BRANCH", "main")
NESSIE_URI = os.environ.get("NESSIE_URI", f"http://nessie:19120/iceberg/{NESSIE_BRANCH}")
ICEBERG_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "s3://warehouse")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
S3_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "minioadmin")
S3_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")

# S3 properties that must be set on the client side.
# Nessie returns its internal Docker hostname (minio:9000) as the S3 endpoint in
# table configs — that hostname is unreachable from the host machine. We override
# these properties on every table object after loading to use the host-visible address.
_S3_OVERRIDES: dict[str, Any] = {
    "s3.endpoint": S3_ENDPOINT,
    "s3.access-key-id": S3_ACCESS_KEY,
    "s3.secret-access-key": S3_SECRET_KEY,
    "s3.path-style-access": "true",
    "s3.remote-signing-enabled": "false",
}


def patch_table_io(table: Table) -> Table:
    """Override S3 connection properties on a loaded Iceberg table.

    Nessie injects its internal Docker hostname (minio:9000) into table
    configs. This function replaces those with the host-visible MinIO address
    so that PyIceberg can write Parquet files from outside Docker.

    Args:
        table: A Table loaded from the Nessie REST catalog.

    Returns:
        The same table object with patched IO properties.
    """
    table.io.properties.update(_S3_OVERRIDES)
    return table


def get_catalog() -> Catalog:
    """Return a connected Iceberg catalog instance (Nessie REST).

    PyIceberg writes Parquet data files directly to MinIO from the client side,
    so S3 credentials must be configured here — not only in Nessie.

    Returns:
        A ready-to-use PyIceberg Catalog connected to Nessie.

    Raises:
        Exception: If Nessie is unreachable or misconfigured.
    """
    return load_catalog("nessie", **{
        "type": "rest",
        "uri": NESSIE_URI,
        "warehouse": ICEBERG_WAREHOUSE,
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": S3_ACCESS_KEY,
        "s3.secret-access-key": S3_SECRET_KEY,
        "s3.path-style-access": "true",
    })
