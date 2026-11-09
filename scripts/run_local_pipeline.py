import sys
import uuid
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from sqlalchemy import text
from config.db_config import get_oltp_conn, get_snowflake_conn
from scripts.generate_oltp_data import generate_data
from scripts.extract_load_snowflake import run_extract_load_snowflake
from scripts.dq_gate_snowflake import run_dq_gate_snowflake
from scripts.post_load_watermark import update_watermark_on_success, mark_batch_failed

DBT_DIR = BASE_DIR / "dbt_project"


def execute_pg_sql_file(conn, file_path: Path):
    conn.execute(text(file_path.read_text(encoding="utf-8")))
    conn.commit()


def execute_sf_sql_file(sf_conn, file_path: Path):
    sql_text = file_path.read_text(encoding="utf-8")
    for cur in sf_conn.execute_string(sql_text):
        pass


def run_dbt_cmd(args: list[str]):
    cmd = ["dbt"] + args + ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    print(f"⚙️ Chạy lệnh dbt: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def init_databases():
    print("🛠️ Đang khởi tạo DDL cho PostgreSQL OLTP và Snowflake DWH...")
    with get_oltp_conn() as oltp_conn:
        execute_pg_sql_file(oltp_conn, BASE_DIR / "sql/oltp/01_create_oltp_tables.sql")
        execute_pg_sql_file(oltp_conn, BASE_DIR / "sql/oltp/02_seed_constraints.sql")

    with get_snowflake_conn(schema="PUBLIC") as sf_conn:
        execute_sf_sql_file(sf_conn, BASE_DIR / "sql/snowflake_init/01_init_snowflake.sql")


def run_pipeline():
    batch_id = f"BATCH_{uuid.uuid4().hex[:10].upper()}"
    dbt_vars = f'{{"batch_id": "{batch_id}"}}'
    print("=" * 75)
    print(f"🚀 KHỞI CHẠY SNOWFLAKE + DBT ELT PIPELINE | BATCH_ID: {batch_id}")
    print("=" * 75)

    init_databases()
    generate_data()

    try:
        # 1. Extract từ Postgres OLTP -> Load vào Snowflake STG
        run_extract_load_snowflake(batch_id)

        # 2. Chạy Data Quality Gate & cách ly lỗi vào META.DATA_QUARANTINE
        run_dq_gate_snowflake(batch_id)

        # 3. Cài đặt dbt packages & chạy tầng Silver
        run_dbt_cmd(["deps"])
        run_dbt_cmd(["run", "--select", "silver", "--vars", dbt_vars])

        # 4. Chạy dbt snapshot để ghi nhận lịch sử SCD Type 2 (Customers & Couriers)
        run_dbt_cmd(["snapshot", "--vars", dbt_vars])

        # 5. Chạy tầng Gold (8 Dimensions -> 3 Facts -> 3 Views/MVs)
        run_dbt_cmd(["run", "--select", "gold.dimensions", "--vars", dbt_vars])
        run_dbt_cmd(["run", "--select", "gold.facts", "--vars", dbt_vars])
        run_dbt_cmd(["run", "--select", "gold.analytics", "--vars", dbt_vars])

        # 6. Kiểm tra chất lượng dữ liệu đầu ra bằng dbt test
        run_dbt_cmd(["test", "--vars", dbt_vars])

        # 7. Cập nhật Watermark từ SOURCE_SNAPSHOT_TS sau khi toàn bộ pipeline thành công
        update_watermark_on_success(batch_id)

    except Exception as exc:
        mark_batch_failed(batch_id, repr(exc))
        print(f"❌ Batch {batch_id} THẤT BẠI: {exc}")
        raise

    with get_snowflake_conn(schema="GOLD") as sf_conn:
        cur = sf_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM GOLD.FACT_SHIPMENT_LIFECYCLE")
        fsl_cnt = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM GOLD.FACT_TRACKING_EVENTS")
        fte_cnt = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM GOLD.FACT_DAILY_HUB_SNAPSHOT")
        fdhs_cnt = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM META.DATA_QUARANTINE")
        quar_cnt = cur.fetchone()[0]
        print("=" * 75)
        print("🎉 SNOWFLAKE + DBT PIPELINE HOÀN TẤT THÀNH CÔNG!")
        print(f"   • GOLD.FACT_SHIPMENT_LIFECYCLE : {fsl_cnt:,} dòng")
        print(f"   • GOLD.FACT_TRACKING_EVENTS    : {fte_cnt:,} dòng")
        print(f"   • GOLD.FACT_DAILY_HUB_SNAPSHOT : {fdhs_cnt:,} dòng")
        print(f"   • META.DATA_QUARANTINE         : {quar_cnt:,} bản ghi lỗi/cảnh báo đã bắt được")
        print("=" * 75)


if __name__ == "__main__":
    run_pipeline()