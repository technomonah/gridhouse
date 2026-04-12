"""Shared pytest fixtures for transformation unit tests.

Uses DuckDB in-memory for fast SQL testing — no JVM or SparkSession needed.
DuckDB supports the same standard SQL (window functions, MD5, LOWER, TRIM)
used in the Silver models, making the dedup logic fully testable here.

End-to-end Spark integration is verified manually via `make transform`.
"""

import pytest
import duckdb


@pytest.fixture
def db():
    """Provide a fresh in-memory DuckDB connection per test.

    Returns:
        duckdb.DuckDBPyConnection: Ready-to-query in-memory database.
            Closed automatically after each test.
    """
    conn = duckdb.connect(database=":memory:")
    yield conn
    conn.close()
