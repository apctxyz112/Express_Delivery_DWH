import uuid
from pathlib import Path
from sqlalchemy import text
from config.db_config import get_oltp_conn, get_dwh_conn
from scripts.generate_oltp_data import generate_data
from scripts.extract_incremental import run_extract_incremental
from scripts.data_quality_checks import run_data_quality_and_silver_load
from scripts.load_dimensions import run_load_dimensions
from scripts.load_facts import run_load_facts
from scripts.refresh_views import run_post_load_and_refresh_views

BASE_DIR = Path(__file__).resolve().parent.parent

def execute_sql_file(conn, file_path: Path):
    sql_content = file_path.read_text(encoding="utf-8")
    
    # FIX: Dùng exec_driver_sql để bypass SQLAlchemy, gửi thẳng xuống psycopg2 
    # giúp chạy được nhiều câu lệnh DDL (multiple statements) trong cùng 1 file .sql
    conn.exec_driver_sql(sql_content)
    conn.commit()

def init_databases():
    print("🛠️ Đang khởi tạo DDL cho OLTP và DWH...")
    with get_oltp_conn() as oltp_conn:
        execute_sql_file(oltp_conn, BASE_DIR / "sql/oltp/01_create_oltp_tables.sql")
        execute_sql_file(oltp_conn, BASE_DIR / "sql/oltp/02_seed_constraints.sql")

    with get_dwh_conn() as dwh_conn:
        for sql_file in sorted((BASE_DIR / "sql/dwh").glob("*.sql")):
            execute_sql_file(dwh_conn, sql_file)
        for sql_file in sorted((BASE_DIR / "sql/analytics").glob("*.sql")):
            execute_sql_file(dwh_conn, sql_file)

def run_pipeline():
    batch_id = f"BATCH_{uuid.uuid4().hex[:10].upper()}"
    print("=" * 75)
    print(f"🚀 KHỞI CHẠY EXPRESS DELIVERY DWH PIPELINE | BATCH_ID: {batch_id}")
    print("=" * 75)

    init_databases()
    generate_data()
    run_extract_incremental(batch_id)
    run_data_quality_and_silver_load(batch_id)
    run_load_dimensions(batch_id)
    run_load_facts(batch_id)
    run_post_load_and_refresh_views(batch_id)

    # In báo cáo tổng kết
    with get_dwh_conn() as conn:
        fsl_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_shipment_lifecycle")).scalar()
        fte_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_tracking_events")).scalar()
        fdhs_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_daily_hub_snapshot")).scalar()
        quar_cnt = conn.execute(text("SELECT COUNT(*) FROM meta.data_quarantine")).scalar()
        print("=" * 75)
        print(f"🎉 PIPELINE HOÀN TẤT THÀNH CÔNG!")
        print(f"   • Fact_Shipment_Lifecycle : {fsl_cnt:,} dòng")
        print(f"   • Fact_Tracking_Events    : {fte_cnt:,} dòng")
        print(f"   • Fact_Daily_Hub_Snapshot : {fdhs_cnt:,} dòng")
        print(f"   • Meta.Data_Quarantine    : {quar_cnt:,} bản ghi lỗi/cảnh báo đã bắt được")
        print("=" * 75)

if __name__ == "__main__":
    run_pipeline()