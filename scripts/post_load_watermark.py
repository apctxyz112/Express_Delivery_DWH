from config.db_config import get_snowflake_conn
from scripts.extract_load_snowflake import TABLES_TO_EXTRACT


def update_watermark_on_success(batch_id: str):
    print(f"🔄 [Batch: {batch_id}] Cập nhật Watermark & Hoàn tất Batch Audit Log trên Snowflake...")
    with get_snowflake_conn(schema="META") as sf_conn:
        cur = sf_conn.cursor()
        for table in TABLES_TO_EXTRACT:
            # [AUDIT-A3] Watermark mới = SOURCE_SNAPSHOT_TS của batch, không dùng MAX(updated_at)
            cur.execute("""
                UPDATE META.ETL_WATERMARK w
                SET LAST_EXTRACTED_TIMESTAMP = COALESCE(b.SOURCE_SNAPSHOT_TS, w.LAST_EXTRACTED_TIMESTAMP),
                    LAST_BATCH_ID = %s,
                    UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                FROM META.ETL_BATCH_LOG b
                WHERE w.TABLE_NAME = %s
                    AND b.BATCH_ID = %s
                    AND b.TABLE_NAME = w.TABLE_NAME
            """, (batch_id, table, batch_id))

            cur.execute("""
                UPDATE META.ETL_BATCH_LOG
                SET RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                    ROWS_LOADED = GREATEST(ROWS_EXTRACTED - ROWS_REJECTED, 0),
                    STATUS = 'SUCCESS'
                WHERE BATCH_ID = %s AND TABLE_NAME = %s
            """, (batch_id, table))
    print("✅ Đã cập nhật Watermark và đóng Batch thành công!")


def mark_batch_failed(batch_id: str, error_message: str):
    """[AUDIT-G1] Đánh dấu FAILED cho mọi bảng chưa SUCCESS khi một bước giữa chừng bị lỗi."""
    with get_snowflake_conn(schema="META") as sf_conn:
        cur = sf_conn.cursor()
        cur.execute("""
            UPDATE META.ETL_BATCH_LOG
            SET STATUS = 'FAILED',
                RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                ERROR_MESSAGE = LEFT(%s, 4000)
            WHERE BATCH_ID = %s AND STATUS <> 'SUCCESS'
        """, (error_message, batch_id))