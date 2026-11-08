from sqlalchemy import text
from config.db_config import get_dwh_conn

def run_data_quality_and_silver_load(batch_id: str):
    print(f"🛡️ [Batch: {batch_id}] Chạy Data Quality Gate & Chuẩn hóa dữ liệu vào tầng Silver...")

    with get_dwh_conn() as conn:
        # Xóa bản ghi quarantine cũ nếu chạy lại cùng batch_id (Idempotent)
        conn.execute(text("DELETE FROM meta.data_quarantine WHERE batch_id = :batch_id"), {"batch_id": batch_id})

        # =====================================================================
        # 1. KIỂM TRA RULES TRÊN BẢNG SHIPMENTS (BLOCKER RULES)
        # =====================================================================
        # Rule 1: weight <= 0 (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', shipment_number,
                'RULE_WEIGHT_POSITIVE', 'Blocker',
                'Trọng lượng vận đơn nhỏ hơn hoặc bằng 0 kg',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id AND (s.weight IS NULL OR s.weight <= 0);
        """), {"batch_id": batch_id})

        # Rule 2: actual_delivery_date < order_date (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', shipment_number,
                'RULE_DELIVERY_AFTER_ORDER', 'Blocker',
                'Thời gian giao hàng thực tế nhỏ hơn thời gian đặt hàng',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND s.actual_delivery_date IS NOT NULL
              AND s.actual_delivery_date < s.order_date;
        """), {"batch_id": batch_id})

        # Rule 3: Thiếu sender_customer_id hoặc trạng thái không hợp lệ (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', COALESCE(shipment_number, 'UNKNOWN'),
                'RULE_VALID_SHIPMENT_CORE', 'Blocker',
                'Thiếu khách gửi hoặc trạng thái vận đơn không nằm trong danh mục chuẩn',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND (
                  s.sender_customer_id IS NULL
                  OR s.current_status NOT IN ('CREATED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERING', 'DELIVERED', 'RETURNED', 'CANCELLED')
              );
        """), {"batch_id": batch_id})

        # =====================================================================
        # 2. KIỂM TRA RULES TRÊN BẢNG TRACKING_EVENTS & CUSTOMERS (BLOCKER + WARNING)
        # =====================================================================
        # Rule 4: event_time < order_date (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipment_tracking_events', CAST(e.event_id AS VARCHAR),
                'RULE_EVENT_TIME_VALID', 'Blocker',
                'Thời gian quét sự kiện nhỏ hơn thời gian tạo vận đơn gốc',
                to_jsonb(e)
            FROM stg.shipment_tracking_events e
            JOIN stg.shipments s ON e.shipment_id = s.shipment_id AND s.batch_id = :batch_id
            WHERE e.batch_id = :batch_id AND e.event_time < s.order_date;
        """), {"batch_id": batch_id})

        # Rule 5: Thiếu courier_id hoặc hub_id trong tracking_events (Warning - Vẫn cho qua Silver & Map Unknown -1)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipment_tracking_events', CAST(e.event_id AS VARCHAR),
                'RULE_MISSING_HUB_OR_COURIER', 'Warning',
                'Sự kiện thiếu hub_id hoặc courier_id -> Sẽ map vào dòng Unknown (SK = -1)',
                to_jsonb(e)
            FROM stg.shipment_tracking_events e
            WHERE e.batch_id = :batch_id AND (e.hub_id IS NULL OR e.courier_id IS NULL);
        """), {"batch_id": batch_id})

        # Rule 6: Số điện thoại sai định dạng ở bảng customers (Warning - Chuẩn hóa lại số điện thoại)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'customers', customer_code,
                'RULE_INVALID_PHONE_FORMAT', 'Warning',
                'Số điện thoại chứa ký tự không hợp lệ -> Đã được chuẩn hóa tại tầng Silver',
                to_jsonb(c)
            FROM stg.customers c
            WHERE c.batch_id = :batch_id AND c.phone_number !~ '^[0-9]{9,12}$';
        """), {"batch_id": batch_id})

        # =====================================================================
        # 3. NẠP DỮ LIỆU HỢP LỆ TỪ STG SANG SILVER (UPSERT + TÍNH ROW_HASH CHO SCD2)
        # =====================================================================
        # 3.1. Silver Customers
        conn.execute(text("""
            INSERT INTO silver.customers (
                customer_id, customer_code, customer_name, customer_type,
                membership_tier, phone_number, city, district, row_hash, updated_at, batch_id
            )
            SELECT DISTINCT ON (customer_id)
                customer_id,
                TRIM(customer_code),
                TRIM(customer_name),
                TRIM(customer_type),
                TRIM(membership_tier),
                CASE
                    WHEN REGEXP_REPLACE(COALESCE(phone_number, ''), '[^0-9]', '', 'g') = '' THEN '0000000000'
                    ELSE SUBSTRING(REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g') FROM 1 FOR 15)
                END AS phone_number,
                TRIM(city),
                TRIM(district),
                MD5(
                    COALESCE(TRIM(customer_name), '') || '|' ||
                    COALESCE(TRIM(customer_type), '') || '|' ||
                    COALESCE(TRIM(membership_tier), '') || '|' ||
                    COALESCE(TRIM(city), '') || '|' ||
                    COALESCE(TRIM(district), '')
                ) AS row_hash,
                updated_at,
                :batch_id
            FROM stg.customers
            WHERE batch_id = :batch_id
            ORDER BY customer_id, updated_at DESC
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                customer_type = EXCLUDED.customer_type,
                membership_tier = EXCLUDED.membership_tier,
                phone_number = EXCLUDED.phone_number,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                row_hash = EXCLUDED.row_hash,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.2. Silver Hubs
        conn.execute(text("""
            INSERT INTO silver.hubs (hub_id, hub_code, hub_name, hub_level, city, district, capacity, updated_at, batch_id)
            SELECT DISTINCT ON (hub_id)
                hub_id, TRIM(hub_code), TRIM(hub_name), TRIM(hub_level),
                TRIM(city), TRIM(district), capacity, updated_at, :batch_id
            FROM stg.hubs
            WHERE batch_id = :batch_id
            ORDER BY hub_id, updated_at DESC
            ON CONFLICT (hub_id) DO UPDATE SET
                hub_name = EXCLUDED.hub_name,
                hub_level = EXCLUDED.hub_level,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                capacity = EXCLUDED.capacity,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.3. Silver Vehicles
        conn.execute(text("""
            INSERT INTO silver.vehicles (vehicle_id, vehicle_code, vehicle_type, capacity_kg, updated_at, batch_id)
            SELECT DISTINCT ON (vehicle_id)
                vehicle_id, TRIM(vehicle_code), TRIM(vehicle_type), capacity_kg, updated_at, :batch_id
            FROM stg.vehicles
            WHERE batch_id = :batch_id
            ORDER BY vehicle_id, updated_at DESC
            ON CONFLICT (vehicle_id) DO UPDATE SET
                vehicle_type = EXCLUDED.vehicle_type,
                capacity_kg = EXCLUDED.capacity_kg,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.4. Silver Couriers
        conn.execute(text("""
            INSERT INTO silver.couriers (
                courier_id, courier_code, courier_name, hub_id,
                contract_type, vehicle_id, row_hash, updated_at, batch_id
            )
            SELECT DISTINCT ON (courier_id)
                courier_id,
                TRIM(courier_code),
                TRIM(courier_name),
                hub_id,
                TRIM(contract_type),
                vehicle_id,
                MD5(
                    COALESCE(TRIM(courier_name), '') || '|' ||
                    COALESCE(CAST(hub_id AS VARCHAR), '-1') || '|' ||
                    COALESCE(TRIM(contract_type), '') || '|' ||
                    COALESCE(CAST(vehicle_id AS VARCHAR), '-1')
                ) AS row_hash,
                updated_at,
                :batch_id
            FROM stg.couriers
            WHERE batch_id = :batch_id
            ORDER BY courier_id, updated_at DESC
            ON CONFLICT (courier_id) DO UPDATE SET
                courier_name = EXCLUDED.courier_name,
                hub_id = EXCLUDED.hub_id,
                contract_type = EXCLUDED.contract_type,
                vehicle_id = EXCLUDED.vehicle_id,
                row_hash = EXCLUDED.row_hash,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.5. Silver Service Types, Payment Methods, Status Reasons
        conn.execute(text("""
            INSERT INTO silver.service_types (service_type_id, service_code, service_name, sla_hours, is_express, updated_at, batch_id)
            SELECT DISTINCT ON (service_type_id)
                service_type_id, TRIM(service_code), TRIM(service_name), sla_hours, is_express, updated_at, :batch_id
            FROM stg.service_types WHERE batch_id = :batch_id
            ORDER BY service_type_id, updated_at DESC
            ON CONFLICT (service_type_id) DO UPDATE SET
                service_name = EXCLUDED.service_name, sla_hours = EXCLUDED.sla_hours,
                is_express = EXCLUDED.is_express, updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id;

            INSERT INTO silver.payment_methods (payment_method_id, payment_method_code, payment_method_name, payer_type, has_cod, updated_at, batch_id)
            SELECT DISTINCT ON (payment_method_id)
                payment_method_id, TRIM(payment_method_code), TRIM(payment_method_name), TRIM(payer_type), has_cod, updated_at, :batch_id
            FROM stg.payment_methods WHERE batch_id = :batch_id
            ORDER BY payment_method_id, updated_at DESC
            ON CONFLICT (payment_method_id) DO UPDATE SET
                payment_method_name = EXCLUDED.payment_method_name, payer_type = EXCLUDED.payer_type,
                has_cod = EXCLUDED.has_cod, updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id;

            INSERT INTO silver.status_reasons (status_reason_id, status_code, status_name, reason_code, reason_name, event_category, updated_at, batch_id)
            SELECT DISTINCT ON (status_reason_id)
                status_reason_id, TRIM(status_code), TRIM(status_name), TRIM(reason_code), TRIM(reason_name), TRIM(event_category), updated_at, :batch_id
            FROM stg.status_reasons WHERE batch_id = :batch_id
            ORDER BY status_reason_id, updated_at DESC
            ON CONFLICT (status_reason_id) DO UPDATE SET
                status_name = EXCLUDED.status_name, reason_name = EXCLUDED.reason_name,
                event_category = EXCLUDED.event_category, updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.6. Silver Shipments (Loại bỏ tất cả các bản ghi dính lỗi Blocker trong Quarantine)
        conn.execute(text("""
            INSERT INTO silver.shipments (
                shipment_id, shipment_number, sender_customer_id, receiver_customer_id,
                origin_hub_id, destination_hub_id, service_type_id, payment_method_id,
                weight, distance_km, freight_charge, cod_amount,
                order_date, pickup_date, promised_delivery_date, actual_delivery_date,
                current_status, updated_at, batch_id
            )
            SELECT DISTINCT ON (s.shipment_id)
                s.shipment_id, TRIM(s.shipment_number), s.sender_customer_id, s.receiver_customer_id,
                s.origin_hub_id, s.destination_hub_id, s.service_type_id, s.payment_method_id,
                s.weight, COALESCE(s.distance_km, 10.0), s.freight_charge, COALESCE(s.cod_amount, 0),
                s.order_date, s.pickup_date, s.promised_delivery_date, s.actual_delivery_date,
                TRIM(s.current_status), s.updated_at, :batch_id
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND NOT EXISTS (
                  SELECT 1 FROM meta.data_quarantine q
                  WHERE q.batch_id = :batch_id
                    AND q.source_table = 'shipments'
                    AND q.severity = 'Blocker'
                    AND q.record_id = s.shipment_number
              )
            ORDER BY s.shipment_id, s.updated_at DESC
            ON CONFLICT (shipment_id) DO UPDATE SET
                pickup_date = EXCLUDED.pickup_date,
                actual_delivery_date = EXCLUDED.actual_delivery_date,
                current_status = EXCLUDED.current_status,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.7. Silver Tracking Events (Chỉ giữ events của các Shipments hợp lệ và không dính Blocker)
        conn.execute(text("""
            INSERT INTO silver.shipment_tracking_events (
                event_id, shipment_id, event_time, event_type,
                hub_id, courier_id, vehicle_id, status_reason_id,
                scan_source, updated_at, batch_id
            )
            SELECT DISTINCT ON (e.event_id)
                e.event_id, e.shipment_id, e.event_time, TRIM(e.event_type),
                e.hub_id, e.courier_id, e.vehicle_id, e.status_reason_id,
                TRIM(e.scan_source), e.updated_at, :batch_id
            FROM stg.shipment_tracking_events e
            JOIN silver.shipments ss ON e.shipment_id = ss.shipment_id
            WHERE e.batch_id = :batch_id
              AND NOT EXISTS (
                  SELECT 1 FROM meta.data_quarantine q
                  WHERE q.batch_id = :batch_id
                    AND q.source_table = 'shipment_tracking_events'
                    AND q.severity = 'Blocker'
                    AND q.record_id = CAST(e.event_id AS VARCHAR)
              )
            ORDER BY e.event_id, e.updated_at DESC
            ON CONFLICT (event_id) DO UPDATE SET
                event_time = EXCLUDED.event_time,
                event_type = EXCLUDED.event_type,
                hub_id = EXCLUDED.hub_id,
                courier_id = EXCLUDED.courier_id,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.8. Silver COD Settlements
        conn.execute(text("""
            INSERT INTO silver.cod_settlements (
                settlement_id, shipment_id, cod_amount, settlement_status, settled_at, updated_at, batch_id
            )
            SELECT DISTINCT ON (c.settlement_id)
                c.settlement_id, c.shipment_id, c.cod_amount, TRIM(c.settlement_status), c.settled_at, c.updated_at, :batch_id
            FROM stg.cod_settlements c
            JOIN silver.shipments ss ON c.shipment_id = ss.shipment_id
            WHERE c.batch_id = :batch_id
            ORDER BY c.settlement_id, c.updated_at DESC
            ON CONFLICT (settlement_id) DO UPDATE SET
                settlement_status = EXCLUDED.settlement_status,
                settled_at = EXCLUDED.settled_at,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # Cập nhật số dòng bị từ chối (Blocker) vào meta.etl_batch_log
        conn.execute(text("""
            UPDATE meta.etl_batch_log b
            SET rows_rejected = (
                SELECT COUNT(*) FROM meta.data_quarantine q
                WHERE q.batch_id = b.batch_id AND q.source_table = b.table_name AND q.severity = 'Blocker'
            )
            WHERE b.batch_id = :batch_id;
        """), {"batch_id": batch_id})

        blocker_cnt = conn.execute(text("""
            SELECT COUNT(*) FROM meta.data_quarantine WHERE batch_id = :batch_id AND severity = 'Blocker'
        """), {"batch_id": batch_id}).scalar()
        warning_cnt = conn.execute(text("""
            SELECT COUNT(*) FROM meta.data_quarantine WHERE batch_id = :batch_id AND severity = 'Warning'
        """), {"batch_id": batch_id}).scalar()

        conn.commit()
        print(f"✅ Data Quality Gate hoàn tất: {blocker_cnt} bản ghi Blocker bị cách ly, {warning_cnt} cảnh báo Warning đã được xử lý.")