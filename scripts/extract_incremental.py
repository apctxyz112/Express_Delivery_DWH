import pandas as pd
from datetime import datetime
from sqlalchemy import text
from config.db_config import get_oltp_conn, get_dwh_conn

TABLES_TO_EXTRACT = [
    "customers", "hubs", "vehicles", "couriers", "service_types",
    "payment_methods", "status_reasons", "shipments",
    "shipment_tracking_events", "cod_settlements"
]

def run_extract_incremental(batch_id: str):
    print(f"📥 [Batch: {batch_id}] Bắt đầu Incremental Extract từ OLTP sang Staging...")
    extracted_counts = {}

    with get_dwh_conn() as dwh_conn, get_oltp_conn() as oltp_conn:
        for table in TABLES_TO_EXTRACT:
            run_start = datetime.now()
            dwh_conn.execute(text("""
                INSERT INTO meta.etl_batch_log (batch_id, table_name, run_start, status)
                VALUES (:batch_id, :table_name, :run_start, 'RUNNING')
                ON CONFLICT (batch_id, table_name) DO UPDATE
                SET run_start = EXCLUDED.run_start, status = 'RUNNING', error_message = NULL
            """), {"batch_id": batch_id, "table_name": table, "run_start": run_start})
            dwh_conn.commit()

            try:
                wm_ts = dwh_conn.execute(text("""
                    SELECT last_extracted_timestamp FROM meta.etl_watermark WHERE table_name = :table_name
                """), {"table_name": table}).scalar() or datetime(1970, 1, 1)

                # CHUẨN ENTERPRISE: Dùng text() và bindparams()
                query = text(f"SELECT * FROM {table} WHERE updated_at > :wm_ts").bindparams(wm_ts=wm_ts)
                
                # Pandas 2.1.4 + SQLAlchemy 1.4 (future=True) = Hoạt động hoàn hảo
                df = pd.read_sql(query, oltp_conn)
                row_count = len(df)
                extracted_counts[table] = row_count

                dwh_conn.execute(text(f"DELETE FROM stg.{table} WHERE batch_id = :batch_id"), {"batch_id": batch_id})

                if row_count > 0:
                    df["batch_id"] = batch_id
                    df["extracted_at"] = datetime.now()
                    df.to_sql(
                        name=table, con=dwh_conn, schema="stg",
                        if_exists="append", index=False, method="multi", chunksize=2000
                    )

                dwh_conn.execute(text("""
                    UPDATE meta.etl_batch_log
                    SET rows_extracted = :rows_extracted, status = 'EXTRACTED'
                    WHERE batch_id = :batch_id AND table_name = :table_name
                """), {"rows_extracted": row_count, "batch_id": batch_id, "table_name": table})
                dwh_conn.commit()
                print(f"   ✔ {table}: Extracted {row_count} rows (Watermark > {wm_ts})")

            except Exception as exc:
                dwh_conn.execute(text("""
                    UPDATE meta.etl_batch_log
                    SET run_end = CURRENT_TIMESTAMP, status = 'FAILED', error_message = :err
                    WHERE batch_id = :batch_id AND table_name = :table_name
                """), {"err": str(exc), "batch_id": batch_id, "table_name": table})
                dwh_conn.commit()
                raise

    return extracted_counts