from config.db_config import get_snowflake_conn


def _table_exists(cur, schema: str, table_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM EXPRESS_DWH.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (schema, table_name),
    )
    return cur.fetchone()[0] > 0


VALID_SHIPMENT_STATUSES = (
    "('CREATED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERING', "
    "'DELIVERY_FAILED', 'DELIVERED', 'RETURNED', 'CANCELLED')"
)


def run_dq_gate_snowflake(batch_id: str):
    print(f"🛡️ [Batch: {batch_id}] Chạy Data Quality Gate & Quarantine trên Snowflake...")

    with get_snowflake_conn(schema="META") as sf_conn:
        cur = sf_conn.cursor()

        # Kiểm tra bảng SILVER_SHIPMENTS và SILVER_SHIPMENT_TRACKING_EVENTS (tên bảng do dbt tạo)
        silver_shipments_exists = _table_exists(cur, "SILVER", "SILVER_SHIPMENTS")
        silver_events_exists = _table_exists(cur, "SILVER", "SILVER_SHIPMENT_TRACKING_EVENTS")

        # Idempotent khi chạy lại cùng batch_id
        cur.execute("DELETE FROM META.DATA_QUARANTINE WHERE BATCH_ID = %s", (batch_id,))

        # Rule 1: weight <= 0 (Blocker) - [AUDIT-B1] record_id = SHIPMENT_ID
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_WEIGHT_POSITIVE', 'Blocker',
                'Trọng lượng vận đơn nhỏ hơn hoặc bằng 0 kg',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s AND (s.WEIGHT IS NULL OR s.WEIGHT <= 0)
        """, (batch_id, batch_id))

        # Rule 2: actual_delivery_date < order_date (Blocker)
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_DELIVERY_AFTER_ORDER', 'Blocker',
                'Thời gian giao hàng thực tế nhỏ hơn thời gian đặt hàng',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s
                AND s.ACTUAL_DELIVERY_DATE IS NOT NULL
                AND s.ACTUAL_DELIVERY_DATE < s.ORDER_DATE
        """, (batch_id, batch_id))

        # Rule 3: Thiếu trường bắt buộc / trạng thái không hợp lệ (Blocker) - [AUDIT-B2]
        cur.execute(f"""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_VALID_SHIPMENT_CORE', 'Blocker',
                'Thiếu số vận đơn/khách gửi/ngày đặt/ngày hẹn giao/cước hoặc trạng thái không nằm trong danh mục chuẩn',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s
                AND (
                    s.SHIPMENT_NUMBER IS NULL
                    OR s.SENDER_CUSTOMER_ID IS NULL
                    OR s.ORDER_DATE IS NULL
                    OR s.PROMISED_DELIVERY_DATE IS NULL
                    OR s.FREIGHT_CHARGE IS NULL
                    OR s.CURRENT_STATUS IS NULL
                    OR TRIM(s.CURRENT_STATUS) NOT IN {VALID_SHIPMENT_STATUSES}
                )
        """, (batch_id, batch_id))

        # Rule 3c: Mốc thời gian ở tương lai so với thời điểm trích xuất (Blocker) - [AUDIT-B9]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_NO_FUTURE_TIMESTAMP', 'Blocker',
                'Ngày đặt / lấy hàng / giao hàng nằm sau thời điểm trích xuất dữ liệu nguồn',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s
                AND GREATEST(
                    s.ORDER_DATE,
                    COALESCE(s.PICKUP_DATE, s.ORDER_DATE),
                    COALESCE(s.ACTUAL_DELIVERY_DATE, s.ORDER_DATE)
                ) > DATEADD('minute', 5, s.EXTRACTED_AT)
        """, (batch_id, batch_id))

        # Rule 3d: Ngày đặt hàng muộn hơn sự kiện đã có ở Silver (Blocker) - [AUDIT-B10]
        if silver_shipments_exists and silver_events_exists:
            cur.execute("""
                INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
                SELECT
                    %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                    'RULE_ORDER_AFTER_EVENTS', 'Blocker',
                    'Ngày đặt hàng muộn hơn sự kiện hành trình đã ghi nhận của chính vận đơn',
                    OBJECT_CONSTRUCT(s.*)
                FROM STG.SHIPMENTS s
                WHERE s.BATCH_ID = %s
                    AND EXISTS (
                        SELECT 1 FROM SILVER.SILVER_SHIPMENT_TRACKING_EVENTS e
                        WHERE e.SHIPMENT_ID = s.SHIPMENT_ID AND e.EVENT_TIME < s.ORDER_DATE
                    )
            """, (batch_id, batch_id))

        # Rule 3b: Thiếu khóa ngoại phụ (Warning -> Map Unknown -1 ở Gold) - [AUDIT-B3]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_MISSING_SHIPMENT_FK', 'Warning',
                'Thiếu người nhận/hub đi/hub đến/dịch vụ/phương thức thanh toán -> Sẽ map vào dòng Unknown (SK = -1)',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s
                AND (
                    s.RECEIVER_CUSTOMER_ID IS NULL OR s.ORIGIN_HUB_ID IS NULL OR s.DESTINATION_HUB_ID IS NULL
                    OR s.SERVICE_TYPE_ID IS NULL OR s.PAYMENT_METHOD_ID IS NULL
                )
        """, (batch_id, batch_id))

        # Rule 4: event_time < order_date (Blocker - Đối chiếu cả STG lẫn SILVER ở mọi lần chạy)
        if silver_shipments_exists:
            cur.execute("""
                INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
                SELECT
                    %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                    'RULE_EVENT_TIME_VALID', 'Blocker',
                    'Thời gian quét sự kiện nhỏ hơn thời gian tạo vận đơn gốc',
                    OBJECT_CONSTRUCT(e.*)
                FROM STG.SHIPMENT_TRACKING_EVENTS e
                LEFT JOIN STG.SHIPMENTS s_stg
                    ON e.SHIPMENT_ID = s_stg.SHIPMENT_ID AND s_stg.BATCH_ID = %s
                LEFT JOIN SILVER.SILVER_SHIPMENTS s_sil
                    ON e.SHIPMENT_ID = s_sil.SHIPMENT_ID
                WHERE e.BATCH_ID = %s
                    AND e.EVENT_TIME < COALESCE(s_stg.ORDER_DATE, s_sil.ORDER_DATE)
            """, (batch_id, batch_id, batch_id))
        else:
            cur.execute("""
                INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
                SELECT
                    %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                    'RULE_EVENT_TIME_VALID', 'Blocker',
                    'Thời gian quét sự kiện nhỏ hơn thời gian tạo vận đơn gốc',
                    OBJECT_CONSTRUCT(e.*)
                FROM STG.SHIPMENT_TRACKING_EVENTS e
                JOIN STG.SHIPMENTS s_stg
                    ON e.SHIPMENT_ID = s_stg.SHIPMENT_ID AND s_stg.BATCH_ID = %s
                WHERE e.BATCH_ID = %s
                    AND e.EVENT_TIME < s_stg.ORDER_DATE
            """, (batch_id, batch_id, batch_id))

        # Rule 4b: Thiếu trường bắt buộc của sự kiện (Blocker) - [AUDIT-B4]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                'RULE_EVENT_REQUIRED_FIELDS', 'Blocker',
                'Sự kiện thiếu shipment_id, event_time hoặc event_type',
                OBJECT_CONSTRUCT(e.*)
            FROM STG.SHIPMENT_TRACKING_EVENTS e
            WHERE e.BATCH_ID = %s
                AND (e.SHIPMENT_ID IS NULL OR e.EVENT_TIME IS NULL OR e.EVENT_TYPE IS NULL)
        """, (batch_id, batch_id))

        # Rule 4c: Sự kiện quét ở tương lai so với thời điểm trích xuất (Blocker) - [AUDIT-B9]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                'RULE_NO_FUTURE_TIMESTAMP', 'Blocker',
                'Thời gian quét sự kiện nằm sau thời điểm trích xuất dữ liệu nguồn',
                OBJECT_CONSTRUCT(e.*)
            FROM STG.SHIPMENT_TRACKING_EVENTS e
            WHERE e.BATCH_ID = %s
                AND e.EVENT_TIME > DATEADD('minute', 5, e.EXTRACTED_AT)
        """, (batch_id, batch_id))

        # Rule 5: Thiếu courier_id hoặc hub_id trong tracking_events (Warning -> Map Unknown -1)
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                'RULE_MISSING_HUB_OR_COURIER', 'Warning',
                'Sự kiện thiếu hub_id hoặc courier_id -> Sẽ map vào dòng Unknown (SK = -1)',
                OBJECT_CONSTRUCT(e.*)
            FROM STG.SHIPMENT_TRACKING_EVENTS e
            WHERE e.BATCH_ID = %s AND (e.HUB_ID IS NULL OR e.COURIER_ID IS NULL)
        """, (batch_id, batch_id))

        # Rule 6: Số điện thoại sai định dạng ở bảng customers (Warning) - [AUDIT-B6]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'customers', c.CUSTOMER_CODE,
                'RULE_INVALID_PHONE_FORMAT', 'Warning',
                'Số điện thoại sai định dạng -> Silver loại ký tự lạ; nếu vẫn không hợp lệ thì gán NULL (không bịa số)',
                OBJECT_CONSTRUCT(c.*)
            FROM STG.CUSTOMERS c
            WHERE c.BATCH_ID = %s AND NOT REGEXP_LIKE(COALESCE(c.PHONE_NUMBER, ''), '^[0-9]{9,12}$')
        """, (batch_id, batch_id))

        # Rule 7: [AUDIT-B7] Bản ghi con thuộc vận đơn chưa hợp lệ ở Silver (Chạy cả ở lần đầu và các lần sau)
        for child_table, child_pk in (
            ("SHIPMENT_TRACKING_EVENTS", "EVENT_ID"),
            ("COD_SETTLEMENTS", "SETTLEMENT_ID"),
            ("CUSTOMER_TICKETS", "TICKET_ID"),
        ):
            silver_check_sql = (
                "AND NOT EXISTS (SELECT 1 FROM SILVER.SILVER_SHIPMENTS ss WHERE ss.SHIPMENT_ID = c.SHIPMENT_ID)"
                if silver_shipments_exists else ""
            )
            cur.execute(f"""
                INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
                SELECT
                    %s, '{child_table.lower()}', TO_VARCHAR(c.{child_pk}),
                    'RULE_ORPHAN_SHIPMENT', 'Warning',
                    'Vận đơn cha chưa có/chưa hợp lệ ở Silver -> Giữ ở Silver, tự lên Gold khi vận đơn cha hợp lệ',
                    OBJECT_CONSTRUCT(c.*)
                FROM STG.{child_table} c
                WHERE c.BATCH_ID = %s
                    AND c.SHIPMENT_ID IS NOT NULL
                    {silver_check_sql}
                    AND NOT EXISTS (
                        SELECT 1 FROM STG.SHIPMENTS s_ok
                        WHERE s_ok.BATCH_ID = %s
                            AND s_ok.SHIPMENT_ID = c.SHIPMENT_ID
                            AND NOT EXISTS (
                                SELECT 1 FROM META.DATA_QUARANTINE q
                                WHERE q.BATCH_ID = %s
                                AND q.SOURCE_TABLE = 'shipments'
                                AND q.SEVERITY = 'Blocker'
                                AND q.RECORD_ID = TO_VARCHAR(s_ok.SHIPMENT_ID)
                            )
                    )
                """, (batch_id, batch_id, batch_id, batch_id))

        # [AUDIT-B8] Cập nhật số dòng bị từ chối (COUNT DISTINCT RECORD_ID) vào META.ETL_BATCH_LOG
        cur.execute("""
            UPDATE META.ETL_BATCH_LOG b
            SET ROWS_REJECTED = (
                SELECT COUNT(DISTINCT q.RECORD_ID)
                FROM META.DATA_QUARANTINE q
                WHERE q.BATCH_ID = b.BATCH_ID
                    AND q.SOURCE_TABLE = b.TABLE_NAME
                    AND q.SEVERITY = 'Blocker'
            )
            WHERE b.BATCH_ID = %s
        """, (batch_id,))

        cur.execute(
            "SELECT COUNT(*) FROM META.DATA_QUARANTINE WHERE BATCH_ID = %s AND SEVERITY = 'Blocker'",
            (batch_id,)
        )
        blocker_cnt = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM META.DATA_QUARANTINE WHERE BATCH_ID = %s AND SEVERITY = 'Warning'",
            (batch_id,)
        )
        warning_cnt = cur.fetchone()[0]
        print(f"✅ Data Quality Gate hoàn tất: {blocker_cnt} Blocker bị cách ly, {warning_cnt} Warning được ghi nhận.")