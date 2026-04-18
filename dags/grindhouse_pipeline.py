"""Grindhouse full pipeline DAG.

Orchestrates the complete job hunting data pipeline:

    scrape_hh ─┐
    scrape_tg  ┼─→ transform → score → export_vacancies
    scrape_li  ┘

Scrape tasks run in parallel (all sources are independent). Transform waits for
all scrapers, then dbt runs inside the Spark container. Score and export run on
the host machine via SSHOperator — claude CLI is only available on the host.

Schedule: @daily for development. Switch to @hourly in production.

Scrape/transform use BashOperator inside the Airflow container.
Score/export use SSHOperator connecting to host.docker.internal via SSH key.

SSH connection configured via Airflow connection 'grindhouse_host' (set on first
run via airflow connections add, or via UI Admin → Connections).

Connections / credentials are injected via environment variables (see
docker-compose.yml for the Airflow service environment block).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    # Retry once after 5 minutes on failure — scrape/network errors are transient
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="grindhouse_pipeline",
    description="Grindhouse job hunting pipeline: scrape → transform → score → export",
    # Hourly schedule — vacancies appear continuously, early response matters.
    schedule="0 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["grindhouse", "pipeline"],
) as dag:

    # -----------------------------------------------------------------------
    # Scrape tasks — run in parallel, all sources are independent
    # -----------------------------------------------------------------------

    # scrape_hh disabled — HH API returns 403 (rate limit / IP ban).
    # Re-enable by uncommenting and adding back to the dependency list below.

    scrape_tg = BashOperator(
        task_id="scrape_tg",
        # Telegram extractor: fetch new messages from job-hunting channels into Bronze
        bash_command="cd /opt/grindhouse && python3 -u extractors/tg.py scrape",
    )

    # scrape_linkedin disabled — LinkedIn detects remote Selenium and invalidates session.
    # Re-enable by uncommenting and adding back to the dependency list below.

    # -----------------------------------------------------------------------
    # Transform — dbt run inside Spark container (all Silver + Gold models)
    # -----------------------------------------------------------------------

    transform = BashOperator(
        task_id="transform",
        # Run dbt via run_dbt.py inside the grindhouse-spark Docker container.
        # The SparkSession is initialized by spark_session.py before dbt starts.
        # Transformations mount: /opt/spark/transformations → ./transformations
        bash_command="docker exec grindhouse-spark python3 -u /opt/spark/transformations/run_dbt.py",
        # dbt can take several minutes on cold start — allow 20 minutes
        execution_timeout=timedelta(minutes=20),
    )

    # -----------------------------------------------------------------------
    # Score / Export / Notify — all run inside Airflow container.
    # Vault is mounted at /opt/vault47, project at /opt/grindhouse.
    # -----------------------------------------------------------------------

    score = BashOperator(
        task_id="score",
        # Score vacancies published within the last 30 days that lack a score.
        bash_command="cd /opt/grindhouse && python3 -u scripts/score_vacancies.py",
        execution_timeout=timedelta(minutes=30),
    )

    # -----------------------------------------------------------------------
    # Export — write apply/priority_apply vacancies to Obsidian vault
    # -----------------------------------------------------------------------

    export_vacancies = BashOperator(
        task_id="export_vacancies",
        # Export vacancies with apply/priority_apply score to hire/vacancy/*.md.
        # Vault mounted at /opt/vault47 via docker-compose volume.
        bash_command="cd /opt/grindhouse && python3 -u scripts/export_vacancies.py --vault-path /opt/vault47",
    )

    # -----------------------------------------------------------------------
    # Notify — send Telegram alert for new priority_apply vacancies
    # -----------------------------------------------------------------------

    notify = BashOperator(
        task_id="notify",
        # Alert for vacancies scored as priority_apply in the last ~70 minutes.
        # Deduplicates via scripts/.notified_hks (mounted via /opt/grindhouse).
        bash_command="cd /opt/grindhouse && python3 -u scripts/notify_vacancies.py",
    )

    # -----------------------------------------------------------------------
    # Task dependencies
    # -----------------------------------------------------------------------

    # Scrape tasks run in parallel, then transform waits for all of them
    # scrape_hh excluded — temporarily disabled (HH API 403)
    # scrape_linkedin excluded — LinkedIn detects remote Selenium
    [scrape_tg] >> transform >> score >> export_vacancies >> notify
