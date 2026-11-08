from sqlalchemy import text
from config.db_config import get_dwh_conn
from scripts.extract_incremental import TABLES_TO_EXTRACT

def run_post_load_and_refresh_views(batch_id: str):
    print(f"🔄 [Batch: {batch_id}] Refresh Materialized Views & Cập nhật Watermark / Audit Log...")

    with get_dwh_conn() as conn:
        # 1. Refresh các Materialized Views trong tầng Gold
        conn.execute(text("REFRESH MATERIALIZED VIEW gold.mv_daily_hub_performance;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW gold.mv_courier_performance;"))

        # 2. Cập nhật Watermark và hoàn tất Batch Log cho từng bảng
        for table in TABLES_TO_EXTRACT:
            conn.execute(text(f"""
                UPDATE meta.etl_watermark
                SET last_extracted_timestamp = COALESCE(
                        (SELECT MAX(updated_at) FROM stg.{table} WHERE batch_id = :batch_id),
                        last_extracted_timestamp
                    ),
                    last_batch_id = :batch_id,
                    updated_at = CURRENT_TIMESTAMP
                WHERE table_name = :table_name;
            """), {"batch_id": batch_id, "table_name": table})

            conn.execute(text(f"""
                UPDATE meta.etl_batch_log
                SET run_end = CURRENT_TIMESTAMP,
                    rows_loaded = GREATEST(rows_extracted - rows_rejected, 0),
                    status = 'SUCCESS'
                WHERE batch_id = :batch_id AND table_name = :table_name;
            """), {"batch_id": batch_id, "table_name": table})

        conn.commit()
        print("✅ Đã refresh Materialized Views và cập nhật Watermark thành công!")