import sys
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.db_config import get_oltp_conn, get_snowflake_conn
from scripts.extract_load_snowflake import run_extract_load_snowflake
from scripts.dq_gate_snowflake import run_dq_gate_snowflake
from scripts.post_load_watermark import update_watermark_on_success, mark_batch_failed

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def _batch_id(context) -> str:
    run_id = context["run_id"]
    batch_id = f"AF_{run_id}"
    return batch_id if len(batch_id) <= 64 else "AF_" + hashlib.md5(run_id.encode("utf-8")).hexdigest()


def _on_task_failure(context):
    mark_batch_failed(_batch_id(context), str(context.get("exception")))


def task_preflight_checks(**context):
    with get_oltp_conn() as oltp_conn:
        oltp_conn.execute(text("SELECT 1 FROM shipments LIMIT 1"))
    with get_snowflake_conn(schema="META") as sf_conn:
        sf_conn.cursor().execute("SELECT 1 FROM META.ETL_WATERMARK LIMIT 1")
    print("✅ Preflight checks: Kết nối Postgres OLTP và Snowflake META đều sẵn sàng.")


def task_extract_load_raw(**context):
    batch_id = _batch_id(context)
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    run_extract_load_snowflake(batch_id)


def task_dq_gate(**context):
    run_dq_gate_snowflake(_batch_id(context))


def task_update_watermark(**context):
    update_watermark_on_success(_batch_id(context))


default_args = {
    "owner": "express_dwh_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": _on_task_failure,
}

with DAG(
    dag_id="express_delivery_snowflake_elt",
    default_args=default_args,
    description="Modern ELT Pipeline (Postgres OLTP -> Snowflake + dbt) cho Express Delivery DWH",
    schedule="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["express_delivery", "snowflake", "dbt", "medallion", "kimball", "scd2"],
) as dag:

    preflight_checks = PythonOperator(
        task_id="preflight_checks",
        python_callable=task_preflight_checks,
    )

    extract_and_load_raw = PythonOperator(
        task_id="extract_postgres_to_snowflake_stg",
        python_callable=task_extract_load_raw,
    )

    data_quality_gate = PythonOperator(
        task_id="snowflake_data_quality_gate",
        python_callable=task_dq_gate,
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            "--select silver --vars '{\"batch_id\": \"{{ ti.xcom_pull(task_ids=\"extract_postgres_to_snowflake_stg\", key=\"batch_id\") }}\"}'"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_scd2",
        bash_command=(
            f"dbt snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            "--vars '{\"batch_id\": \"{{ ti.xcom_pull(task_ids=\"extract_postgres_to_snowflake_stg\", key=\"batch_id\") }}\"}'"
        ),
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold_marts",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            "--select gold.dimensions gold.facts gold.analytics "
            "--vars '{\"batch_id\": \"{{ ti.xcom_pull(task_ids=\"extract_postgres_to_snowflake_stg\", key=\"batch_id\") }}\"}'"
        ),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            "--vars '{\"batch_id\": \"{{ ti.xcom_pull(task_ids=\"extract_postgres_to_snowflake_stg\", key=\"batch_id\") }}\"}'"
        ),
    )

    finalize_watermark = PythonOperator(
        task_id="update_watermark_and_audit_log",
        python_callable=task_update_watermark,
    )

    (
        preflight_checks
        >> extract_and_load_raw
        >> data_quality_gate
        >> dbt_run_silver
        >> dbt_snapshot
        >> dbt_run_gold
        >> dbt_test_marts
        >> finalize_watermark
    )