from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
from snowflake.connector.pandas_tools import write_pandas
from config.db_config import get_oltp_conn, get_snowflake_conn

TABLES_TO_EXTRACT = [
    "customers", "hubs", "vehicles", "couriers", "service_types",
    "payment_methods", "status_reasons", "shipments",
    "shipment_tracking_events", "cod_settlements", "customer_tickets"
]

EXTRACT_LOOKBACK_MINUTES = 10
STG_RETENTION_BATCHES = 3


def run_extract_load_snowflake(batch_id: str):
    print(f"📥 [Batch: {batch_id}] Bắt đầu Incremental Extract từ Postgres OLTP -> Snowflake STG...")
    extracted_counts = {}

    with get_snowflake_conn(schema="META") as sf_conn, get_oltp_conn() as oltp_conn:
        sf_cur = sf_conn.cursor()

        # [AUDIT-A5] Chống 2 batch chạy chồng nhau
        sf_cur.execute("""
            SELECT BATCH_ID FROM META.ETL_BATCH_LOG
            WHERE BATCH_ID <> %s
                AND STATUS IN ('RUNNING', 'EXTRACTED')
                AND RUN_START > DATEADD('hour', -6, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
            ORDER BY RUN_START DESC
            LIMIT 1
        """, (batch_id,))
        running_row = sf_cur.fetchone()
        if running_row:
            raise RuntimeError(
                f"Batch {running_row[0]} đang chạy dở. Chờ batch đó xong hoặc đánh dấu FAILED trong META.ETL_BATCH_LOG."
            )

        # [AUDIT-A1] Đọc cả 11 bảng trong cùng 1 snapshot REPEATABLE READ trên Postgres OLTP
        src_conn = oltp_conn.execution_options(isolation_level="REPEATABLE READ")
        snapshot_ts = src_conn.execute(text("SELECT LOCALTIMESTAMP")).scalar()

        for table in TABLES_TO_EXTRACT:
            sf_cur.execute("""
                MERGE INTO META.ETL_BATCH_LOG tgt
                USING (SELECT %s AS BATCH_ID, %s AS TABLE_NAME) src
                ON tgt.BATCH_ID = src.BATCH_ID AND tgt.TABLE_NAME = src.TABLE_NAME
                WHEN MATCHED THEN UPDATE SET
                    RUN_START = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                    STATUS = 'RUNNING',
                    ERROR_MESSAGE = NULL
                WHEN NOT MATCHED THEN INSERT (BATCH_ID, TABLE_NAME, RUN_START, STATUS)
                    VALUES (src.BATCH_ID, src.TABLE_NAME, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 'RUNNING')
            """, (batch_id, table))

            try:
                sf_cur.execute(
                    "SELECT LAST_EXTRACTED_TIMESTAMP FROM META.ETL_WATERMARK WHERE TABLE_NAME = %s",
                    (table,)
                )
                wm_row = sf_cur.fetchone()
                wm_ts = wm_row[0] if wm_row and wm_row[0] else datetime(1970, 1, 1)
                # [AUDIT-A2] Cửa sổ chồng lấn lookback 10 phút chống sót giao dịch commit muộn
                from_ts = wm_ts - timedelta(minutes=EXTRACT_LOOKBACK_MINUTES)

                query = text(f"SELECT * FROM {table} WHERE updated_at > :from_ts").bindparams(from_ts=from_ts)
                df = pd.read_sql(query, src_conn)
                row_count = len(df)
                extracted_counts[table] = row_count

                # [AUDIT-A4] Xóa dữ liệu nếu chạy lại cùng batch_id + chỉ giữ lại N batch SUCCESS gần nhất
                sf_cur.execute(f"""
                    DELETE FROM STG.{table.upper()}
                    WHERE BATCH_ID = %s
                        OR BATCH_ID IN (
                            WITH ranked_batches AS (
                                SELECT BATCH_ID,
                                        ROW_NUMBER() OVER (ORDER BY RUN_START DESC) AS rn
                                FROM META.ETL_BATCH_LOG
                                WHERE TABLE_NAME = %s AND STATUS = 'SUCCESS'
                            )
                            SELECT BATCH_ID FROM ranked_batches WHERE rn > {STG_RETENTION_BATCHES}
                        )
                """, (batch_id, table))

                if row_count > 0:
                    df["batch_id"] = batch_id
                    df["extracted_at"] = pd.Timestamp(snapshot_ts)
                    df.columns = [c.upper() for c in df.columns]

                    # 1. Xử lý riêng các cột kiểu DATE thuần túy (ví dụ: EFFECTIVE_DATE)
                    # Chuẩn hóa mọi cột DATE / TIMESTAMP sang chuỗi ISO để Snowflake tự cast chuẩn xác 100%
                    for col in df.columns:
                        if col == "EFFECTIVE_DATE":
                            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d")
                        elif (
                            pd.api.types.is_datetime64_any_dtype(df[col])
                            or col.endswith("_DATE")
                            or "TIME" in col
                            or col.endswith("_AT")
                        ):
                            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S.%f")

                    write_pandas(
                        conn=sf_conn,
                        df=df,
                        table_name=table.upper(),
                        schema="STG",
                        auto_create_table=False,
                        quote_identifiers=False,
                    )
                sf_cur.execute("""
                    UPDATE META.ETL_BATCH_LOG
                    SET ROWS_EXTRACTED = %s,
                        SOURCE_SNAPSHOT_TS = %s,
                        STATUS = 'EXTRACTED'
                    WHERE BATCH_ID = %s AND TABLE_NAME = %s
                """, (row_count, snapshot_ts, batch_id, table))
                print(f"   ✔ {table}: Extracted {row_count} rows (updated_at > {from_ts}, snapshot {snapshot_ts})")

            except Exception as exc:
                sf_cur.execute("""
                    UPDATE META.ETL_BATCH_LOG
                    SET RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                        STATUS = 'FAILED',
                        ERROR_MESSAGE = %s
                    WHERE BATCH_ID = %s AND TABLE_NAME = %s
                """, (str(exc)[:4000], batch_id, table))
                raise

        src_conn.rollback()

    return extracted_counts