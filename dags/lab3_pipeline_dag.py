from __future__ import annotations

import logging
import os
import shlex
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------
# Part C: Pipeline Orchestration
#
# This DAG orchestrates:
# A.3 Data ingestion -> A.4 Formatting -> A.5 Exploitation-zone build
# -> B.1/B.2 Model training and MLflow model management.
# ---------------------------------------------------------------------

LOGGER = logging.getLogger(__name__)

# Avoid hardcoded absolute paths.
# This file lives in: <project_root>/dags/lab3_pipeline_dag.py
# Therefore parents[1] is the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_PYTHON = Path(os.getenv("LAB3_PYTHON", sys.executable))

COMMON_ENV = {
    "PYTHONPATH": str(PROJECT_ROOT),
    # Avoids common local Spark hostname binding warnings/issues in WSL.
    "SPARK_LOCAL_IP": "127.0.0.1",
}


def project_command(module: str, args: str = "") -> str:
    """
    Build a command that runs one of the repo's Python modules
    using the repo-local virtual environment.

    Example:
        python -m src.ingestion --source income
    """
    return (
        f"cd {shlex.quote(str(PROJECT_ROOT))} && "
        f"{shlex.quote(str(PROJECT_PYTHON))} -m {module} {args}"
    )
    
def preflight_check():
    """
    Validate that the project can run before starting the pipeline.
    This prevents unclear downstream failures.
    """
    required_paths = [
        PROJECT_ROOT / "config" / "config.yaml",
        PROJECT_ROOT / "src" / "ingestion.py",
        PROJECT_ROOT / "src" / "formatting.py",
        PROJECT_ROOT / "src" / "exploitation.py",
        PROJECT_ROOT / "src" / "training.py",
        PROJECT_PYTHON,
    ]

    missing = [str(path) for path in required_paths if not path.exists()]

    if missing:
        raise FileNotFoundError(
            "Preflight check failed. Missing required files:\n"
            + "\n".join(missing)
        )

    LOGGER.info("Preflight check passed.")


def failure_alert(context):
    """
    Custom failure alert.

    In a production deployment this could send email/Slack.
    For this lab we log a clear failure message, which avoids requiring
    SMTP credentials or hardcoded emails.
    """
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    LOGGER.error(
        "AIRFLOW ALERT: task '%s' failed in DAG run '%s'. Check logs for details.",
        task_instance.task_id if task_instance else "unknown_task",
        dag_run.run_id if dag_run else "unknown_run",
    )

def success_alert(context):
    """
    Custom success/completion alert.

    In a production deployment this could send email/Slack.
    For this lab we log a clear success message when the full pipeline completes.
    """
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    LOGGER.info(
        "AIRFLOW COMPLETION ALERT: final task '%s' succeeded in DAG run '%s'. "
        "The full Lab 3 pipeline completed successfully.",
        task_instance.task_id if task_instance else "unknown_task",
        dag_run.run_id if dag_run else "unknown_run",
    )


default_args = {
    "owner": "lab3-group",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": failure_alert,
}

with DAG(
    dag_id="lab3_big_data_pipeline",
    description="Orchestrates the Lab 3 data lake and MLflow pipeline.",
    default_args=default_args,
    start_date=datetime(2026, 6, 24),
    schedule="0 0 * * *",
    catchup=False,
    tags=["lab3", "airflow", "spark", "mlflow"],
) as dag:
    
    # -------------------------
    # Initial validation checks
    # -------------------------
    preflight = PythonOperator(
        task_id="preflight_validate_project_structure",
        python_callable=preflight_check,
    )
    
    # -------------------------
    # A.3 Landing Zone ingestion
    # -------------------------
    ingest_income = BashOperator(
        task_id="A3_ingest_income_to_landing",
        bash_command=project_command("src.ingestion", "--source income --force"),
        env=COMMON_ENV,
    )

    ingest_density = BashOperator(
        task_id="A3_ingest_density_to_landing",
        bash_command=project_command("src.ingestion", "--source density --force"),
        env=COMMON_ENV,
    )

    ingest_price = BashOperator(
        task_id="A3_ingest_price_to_landing",
        bash_command=project_command("src.ingestion", "--source price --force"),
        env=COMMON_ENV,
    )

    # -------------------------
    # A.4 Formatted Zone
    # -------------------------
    format_income = BashOperator(
        task_id="A4_format_income",
        bash_command=project_command("src.formatting", "--dataset income --force"),
        env=COMMON_ENV,
    )

    format_density = BashOperator(
        task_id="A4_format_density",
        bash_command=project_command("src.formatting", "--dataset density --force"),
        env=COMMON_ENV,
    )

    format_price = BashOperator(
        task_id="A4_format_price",
        bash_command=project_command("src.formatting", "--dataset price --force"),
        env=COMMON_ENV,
    )

    # -------------------------
    # A.5 Exploitation Zone
    # -------------------------
    build_exploitation_zone = BashOperator(
        task_id="A5_build_exploitation_zone",
        bash_command=project_command("src.exploitation", "--force"),
        env=COMMON_ENV,
    )

    # -------------------------
    # B.1 + B.2 Training + MLflow
    # -------------------------
    train_and_manage_models = BashOperator(
        task_id="B1_B2_train_validate_and_register_model",
        bash_command=project_command("src.training"),
        env=COMMON_ENV,
    )
    
    # -------------------------
    # Pipeline complete
    # --------------------------
    pipeline_complete = EmptyOperator(
        task_id="pipeline_complete",
        on_success_callback=success_alert,
    )

    # -------------------------
    # C.2 Dependency management
    # -------------------------
    preflight >> [ingest_income, ingest_density, ingest_price]
    
    ingest_income >> format_income
    ingest_density >> format_density
    ingest_price >> format_price

    [format_income, format_density, format_price] >> build_exploitation_zone
    build_exploitation_zone >> train_and_manage_models >> pipeline_complete


if __name__ == "__main__":
    # Allows quick local debugging:
    # python dags/lab3_pipeline_dag.py
    dag.test()