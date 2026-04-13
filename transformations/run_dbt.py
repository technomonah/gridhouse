"""dbt transformation runner for grindhouse Silver + Gold layers.

Initializes SparkSession then invokes dbt via its Python API.
Must be run inside the grindhouse-spark container where transformations/
is mounted at /opt/spark/transformations.

SparkSession is created before dbt imports — dbt-spark's session method
picks up SparkSession.getActiveSession() automatically.

Usage (via Makefile):
    make transform           # dbt run (all models)
    make transform-plan      # dbt compile (dry-run, no execution)
    make test-dbt            # dbt test

Usage (direct, inside container):
    python /opt/spark/transformations/run_dbt.py
    python /opt/spark/transformations/run_dbt.py --compile
    python /opt/spark/transformations/run_dbt.py --test
    python /opt/spark/transformations/run_dbt.py --select silver
    python /opt/spark/transformations/run_dbt.py --select gold
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Add transformations/ to sys.path so spark_session import resolves.
_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT))

# SparkSession must be created before dbt imports — dbt-spark's session method
# calls SparkSession.getActiveSession() when it first connects.
from spark_session import get_spark  # noqa: E402

spark = get_spark("grindhouse-dbt")
print(f"SparkSession started: {spark.version}")


def main() -> None:
    """Parse CLI arguments and invoke dbt run, compile, or test.

    Args:
        --compile:       Run dbt compile (parse + generate SQL, no execution).
        --test:          Run dbt test (schema + data tests).
        --select <val>:  Limit to a subset of models (e.g. 'silver', 'gold', 'hub_vacancy').
    """
    parser = argparse.ArgumentParser(
        description="Run dbt transformations for grindhouse"
    )
    parser.add_argument(
        "--compile",
        action="store_true",
        help="Compile models without executing (dry-run)",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run dbt tests instead of models",
    )
    parser.add_argument(
        "--select",
        type=str,
        default=None,
        help="dbt node selector (e.g. 'silver', 'gold', '+hub_vacancy')",
    )
    args = parser.parse_args()

    # dbt must be imported after SparkSession is active.
    from dbt.cli.main import dbtRunner  # noqa: E402

    profiles_dir = str(_ROOT)
    project_dir = str(_ROOT)

    runner = dbtRunner()

    if args.compile:
        cli_args = ["compile", "--profiles-dir", profiles_dir, "--project-dir", project_dir]
    elif args.test:
        cli_args = ["test", "--profiles-dir", profiles_dir, "--project-dir", project_dir]
    else:
        cli_args = ["run", "--profiles-dir", profiles_dir, "--project-dir", project_dir]

    if args.select:
        cli_args += ["--select", args.select]

    result = runner.invoke(cli_args)

    if result.exception:
        raise result.exception


if __name__ == "__main__":
    main()
