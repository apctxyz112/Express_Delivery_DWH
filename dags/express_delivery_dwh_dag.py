import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import text

# Đảm bảo Airflow import được các module trong project
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.db_config import get_oltp_conn, get_dwh_conn
from scripts.extract_incremental import run_extract_incremental
from scripts.data_quality_checks import run_data_quality_and_silver_load
from scripts.load_dimensions import run_load_dimensions
from scripts.load_facts import run_load_facts
from scripts.refresh_views import run_post_load_and_refresh_views

def task_preflight_checks(**context):
    with get_oltp_conn() as oltp_conn:
        oltp_conn.execute(text("SELECT 1 FROM shipments LIMIT 1"))
    with get_dwh_conn() as dwh_conn:
        dwh_conn.execute(text("SELECT 1 FROM meta.etl_watermark LIMIT 1"))
    print("✅ Preflight checks: Kết nối OLTP, DWH và các bảng Meta đều sẵn sàng.")

def task_extract_incremental(**context):
    batch_id = f"AF_{context['run_id']}"
    run_extract_incremental(batch_id)

def task_data_quality_gate(**context):
    batch_id = f"AF_{context['run_id']}"
    run_data_quality_and_silver_load(batch_id)

def task_load_dimensions(**context):
    batch_id = f"AF_{context['run_id']}"
    run_load_dimensions(batch_id)

def task_load_facts(**context):
    batch_id = f"AF_{context['run_id']}"
    run_load_facts(batch_id)

def task_refresh_views_and_watermark(**context):
    batch_id = f"AF_{context['run_id']}"
    run_post_load_and_refresh_views(batch_id)

default_args = {
    "owner": "express_dwh_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="express_delivery_dwh_dag",
    default_args=default_args,
    description="Enterprise Medallion + Kimball ETL Pipeline cho Express Delivery DWH",
    schedule_interval="0 */4 * * *",
    catchup=False,
    tags=["express_delivery", "medallion", "kimball", "scd2"]
) as dag:

    preflight_checks = PythonOperator(
        task_id="preflight_checks",
        python_callable=task_preflight_checks
    )

    extract_incremental = PythonOperator(
        task_id="extract_incremental",
        python_callable=task_extract_incremental
    )

    data_quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=task_data_quality_gate
    )

    load_dimensions = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions
    )

    load_facts = PythonOperator(
        task_id="load_facts",
        python_callable=task_load_facts
    )

    refresh_views = PythonOperator(
        task_id="refresh_views_and_update_watermark",
        python_callable=task_refresh_views_and_watermark
    )

    preflight_checks >> extract_incremental >> data_quality_gate >> load_dimensions >> load_facts >> refresh_views