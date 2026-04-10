"""Verify that all infrastructure services are healthy and reachable.

Run after `make up` to confirm the stack is ready for use.
Checks: MinIO API, MinIO bucket, Nessie health, PyIceberg catalog connection.
"""

import sys
import os
from pathlib import Path

import requests
import boto3
from botocore.exceptions import ClientError, EndpointResolutionError
from pyiceberg.catalog import load_catalog

# Load .env from project root
env_path = Path(__file__).parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_BUCKET = "warehouse"
NESSIE_HEALTH_URL = "http://localhost:19120/api/v2/config"
NESSIE_ICEBERG_URI = "http://localhost:19120/iceberg/"

OK = "✓"
FAIL = "✗"


def check_minio_api() -> bool:
    """Check that MinIO S3 API is reachable on port 9000."""
    try:
        resp = requests.get(f"{MINIO_ENDPOINT}/minio/health/live", timeout=5)
        if resp.status_code == 200:
            print(f"  {OK} MinIO API reachable at {MINIO_ENDPOINT}")
            return True
        print(f"  {FAIL} MinIO API returned HTTP {resp.status_code}")
        return False
    except Exception as e:
        print(f"  {FAIL} MinIO API unreachable: {e}")
        return False


def check_minio_bucket() -> bool:
    """Check that the warehouse bucket exists in MinIO."""
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_USER,
            aws_secret_access_key=MINIO_PASSWORD,
            region_name="us-east-1",
        )
        s3.head_bucket(Bucket=MINIO_BUCKET)
        print(f"  {OK} Bucket '{MINIO_BUCKET}' exists")
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        print(f"  {FAIL} Bucket '{MINIO_BUCKET}' not found (error: {code})")
        return False
    except Exception as e:
        print(f"  {FAIL} Could not check bucket: {e}")
        return False


def check_nessie_health() -> bool:
    """Check that Nessie API is reachable and returns valid config."""
    try:
        resp = requests.get(NESSIE_HEALTH_URL, timeout=5)
        if resp.status_code == 200:
            branch = resp.json().get("defaultBranch", "?")
            print(f"  {OK} Nessie reachable, default branch: {branch}")
            return True
        print(f"  {FAIL} Nessie API returned HTTP {resp.status_code}")
        return False
    except Exception as e:
        print(f"  {FAIL} Nessie unreachable: {e}")
        return False


def check_pyiceberg_catalog() -> bool:
    """Check that PyIceberg can connect to the Nessie REST catalog."""
    try:
        catalog = load_catalog("nessie", **{
            "type": "rest",
            "uri": NESSIE_ICEBERG_URI,
            "warehouse": f"s3://{MINIO_BUCKET}",
        })
        # List namespaces to confirm catalog is operational
        namespaces = catalog.list_namespaces()
        print(f"  {OK} PyIceberg connected to Nessie catalog (namespaces: {list(namespaces)})")
        return True
    except Exception as e:
        print(f"  {FAIL} PyIceberg could not connect: {e}")
        return False


def main() -> None:
    """Run all infrastructure checks and exit with non-zero code on failure."""
    print("Verifying gridhouse infrastructure...\n")

    checks = [
        ("MinIO API", check_minio_api),
        ("MinIO bucket", check_minio_bucket),
        ("Nessie health", check_nessie_health),
        ("PyIceberg catalog", check_pyiceberg_catalog),
    ]

    results = []
    for name, fn in checks:
        print(f"[{name}]")
        results.append(fn())
        print()

    passed = sum(results)
    total = len(results)
    print(f"{'All checks passed' if passed == total else f'{passed}/{total} checks passed'}")

    if passed < total:
        print("\nTip: run `make up` and wait ~15s before verifying.")
        sys.exit(1)


if __name__ == "__main__":
    main()
