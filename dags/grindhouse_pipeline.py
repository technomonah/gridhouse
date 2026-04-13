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
from airflow.providers.ssh.operators.ssh import SSHOperator

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
    schedule_interval="0 * * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    # SQLite + SequentialExecutor cannot handle concurrent dag_run updates.
    # Limit to 1 active run to avoid StaleDataError on the dag_run table.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["grindhouse", "pipeline"],
) as dag:

    # -----------------------------------------------------------------------
    # Scrape tasks — run in parallel, all sources are independent
    # -----------------------------------------------------------------------

    scrape_hh = BashOperator(
        task_id="scrape_hh",
        # HH.ru extractor: fetch new vacancies from HH API into Bronze layer
        bash_command="cd /opt/grindhouse && python3 -u extractors/hh.py scrape",
    )

    scrape_tg = BashOperator(
        task_id="scrape_tg",
        # Telegram extractor: fetch new messages from job-hunting channels into Bronze
        bash_command="cd /opt/grindhouse && python3 -u extractors/tg.py scrape",
    )

    scrape_linkedin = BashOperator(
        task_id="scrape_linkedin",
        # LinkedIn extractor: fetch new hiring posts into Bronze layer
        bash_command="cd /opt/grindhouse && python3 -u extractors/linkedin.py scrape",
    )

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
    # Score — run Claude Code scoring on unscored vacancies (via SSH to host)
    # claude CLI is only available on the host, not inside the container.
    # SSHOperator connects to host.docker.internal using the mounted SSH key.
    # -----------------------------------------------------------------------

    _project = "/Users/nikitamanakov/Desktop/vault47/projects/grindhouse/code"
    # SSH sessions on macOS don't load .zshrc — PATH is minimal.
    # Prepend nvm node bin so `claude` CLI resolves correctly.
    _python = "/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"
    _node_bin = "/Users/nikitamanakov/.nvm/versions/node/v22.19.0/bin"
    _env = f"export PATH={_node_bin}:$PATH"

    score = SSHOperator(
        task_id="score",
        ssh_conn_id="grindhouse_host",
        # Score vacancies published within the last 30 days that lack a score.
        # PATH is extended so `claude` CLI (nvm node) resolves inside the script.
        command=f"{_env} && cd {_project} && {_python} -u scripts/score_vacancies.py",
        # Scoring can take time proportional to number of new vacancies
        cmd_timeout=30 * 60,
    )

    # -----------------------------------------------------------------------
    # Export — write apply/priority_apply vacancies to Obsidian vault
    # -----------------------------------------------------------------------

    export_vacancies = SSHOperator(
        task_id="export_vacancies",
        ssh_conn_id="grindhouse_host",
        # Export vacancies with apply/priority_apply score to hire/vacancy/*.md
        # Idempotent: skips files that already exist.
        command=(
            f"{_env} && cd {_project} && {_python} -u scripts/export_vacancies.py"
            " --vault-path /Users/nikitamanakov/Desktop/vault47"
        ),
    )

    # -----------------------------------------------------------------------
    # Notify — send Telegram alert for new priority_apply vacancies
    # -----------------------------------------------------------------------

    notify = SSHOperator(
        task_id="notify",
        ssh_conn_id="grindhouse_host",
        # Alert for vacancies scored as priority_apply in the last ~70 minutes.
        # Deduplicates via .notified_hks file — safe to run on every hourly tick.
        command=(
            f"{_env} && cd {_project} && {_python} -u scripts/notify_vacancies.py"
        ),
    )

    # -----------------------------------------------------------------------
    # Task dependencies
    # -----------------------------------------------------------------------

    # Scrape tasks run in parallel, then transform waits for all of them
    [scrape_hh, scrape_tg, scrape_linkedin] >> transform >> score >> export_vacancies >> notify
