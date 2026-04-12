"""SQLMesh transformation runner for grindhouse Silver layer.

Builds a SparkSession (via config.get_spark), then invokes SQLMesh to plan
and apply all Silver models. Must be run inside the grindhouse-spark container
where transformations/ is mounted at /opt/spark/transformations.

Usage (via Makefile):
    make transform        # apply all Silver models
    make transform-plan   # dry-run — print plan without applying

Usage (direct, inside container):
    python /opt/spark/transformations/run.py
    python /opt/spark/transformations/run.py --plan-only
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add transformations/ to sys.path so 'from config import get_spark' resolves.
_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))

# SparkSession must be created before SQLMesh imports — SQLMesh's Spark
# connection type picks up the active SparkSession from the running process.
from spark_session import get_spark  # noqa: E402


def main() -> None:
    """Parse CLI arguments and run SQLMesh plan/apply.

    Args:
        --plan-only: If set, prints the SQLMesh plan without applying it.
    """
    parser = argparse.ArgumentParser(
        description="Run SQLMesh Silver transformations for grindhouse"
    )
    parser.add_argument(
        "--plan-only",
        action="store_true",
        help="Print the SQLMesh execution plan without applying it",
    )
    args = parser.parse_args()

    # Initialize SparkSession — must happen before importing sqlmesh.Context
    spark = get_spark("grindhouse-silver")
    print(f"SparkSession started: {spark.version}")

    import sqlmesh  # noqa: F401 — imported after SparkSession is ready

    ctx = sqlmesh.Context(paths=[str(_ROOT)])

    if args.plan_only:
        ctx.plan(auto_apply=False, no_prompts=True)
    else:
        ctx.plan(auto_apply=True, no_prompts=True)


if __name__ == "__main__":
    main()
