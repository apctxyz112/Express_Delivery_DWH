from sqlalchemy import text
from config.db_config import get_dwh_conn

def run_load_dimensions(batch_id: str):
    print(f"🏛️ [Batch: {batch_id}] Đang nạp 8 bảng Dimensions (SCD Type 1 & SCD Type 2)...")

    with get_dwh_conn() as conn:
        # 1. Nạp Dim_Date (Tự động khởi tạo lịch từ 2024 đến 2028)
        conn.execute(text("""
            INSERT INTO gold.dim_date (
                date_key, full_date, year, quarter, month, month_name,
                week, day_of_week, day_of_month, is_weekend, is_peak_season
            )
            SELECT
                TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
                d::DATE AS full_date,
                EXTRACT(YEAR FROM d)::INT AS year,
                EXTRACT(QUARTER FROM d)::INT AS quarter,
                EXTRACT(MONTH FROM d)::INT AS month,
                TRIM(TO_CHAR(d, 'Month')) AS month_name,
                EXTRACT(WEEK FROM d)::INT AS week,
                EXTRACT(ISODOW FROM d)::INT AS day_of_week,
                EXTRACT(DAY FROM d)::INT AS day_of_month,
                CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
                CASE WHEN EXTRACT(MONTH FROM d) IN (1, 11, 12) THEN TRUE ELSE FALSE END AS is_peak_season
            FROM GENERATE_SERIES('2024-01-01'::DATE, '2028-12-31'::DATE, '1 day'::INTERVAL) AS d
            ON CONFLICT (date_key) DO NOTHING;
        """))

        # 2. Nạp Dim_Hub (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_hub (hub_nk, hub_name, hub_level, city, district, capacity, is_current)
            SELECT hub_code, hub_name, hub_level, city, district, capacity, TRUE
            FROM silver.hubs
            WHERE batch_id = :batch_id
            ON CONFLICT (hub_nk) DO UPDATE SET
                hub_name = EXCLUDED.hub_name,
                hub_level = EXCLUDED.hub_level,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                capacity = EXCLUDED.capacity;
        """), {"batch_id": batch_id})

        # 3. Nạp Dim_Vehicle (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_vehicle (vehicle_nk, vehicle_type, capacity_kg)
            SELECT vehicle_code, vehicle_type, capacity_kg
            FROM silver.vehicles
            WHERE batch_id = :batch_id
            ON CONFLICT (vehicle_nk) DO UPDATE SET
                vehicle_type = EXCLUDED.vehicle_type,
                capacity_kg = EXCLUDED.capacity_kg;
        """), {"batch_id": batch_id})

        # 4. Nạp Dim_Service_Type (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_service_type (service_type_nk, service_name, sla_hours, is_express)
            SELECT service_code, service_name, sla_hours, is_express
            FROM silver.service_types
            WHERE batch_id = :batch_id
            ON CONFLICT (service_type_nk) DO UPDATE SET
                service_name = EXCLUDED.service_name,
                sla_hours = EXCLUDED.sla_hours,
                is_express = EXCLUDED.is_express;
        """), {"batch_id": batch_id})

        # 5. Nạp Dim_Status_Reason (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_status_reason (status_code, status_name, reason_code, reason_name, event_category)
            SELECT status_code, status_name, reason_code, reason_name, event_category
            FROM silver.status_reasons
            WHERE batch_id = :batch_id
            ON CONFLICT (reason_code) DO UPDATE SET
                status_code = EXCLUDED.status_code,
                status_name = EXCLUDED.status_name,
                reason_name = EXCLUDED.reason_name,
                event_category = EXCLUDED.event_category;
        """), {"batch_id": batch_id})

        # 6. Nạp Dim_Payment_Method (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_payment_method (payment_method_nk, payment_method_name, payer_type, has_cod)
            SELECT payment_method_code, payment_method_name, payer_type, has_cod
            FROM silver.payment_methods
            WHERE batch_id = :batch_id
            ON CONFLICT (payment_method_nk) DO UPDATE SET
                payment_method_name = EXCLUDED.payment_method_name,
                payer_type = EXCLUDED.payer_type,
                has_cod = EXCLUDED.has_cod;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 7. NẠP DIM_CUSTOMER (SCD TYPE 2 BẰNG ROW_HASH)
        # =====================================================================
        # Bước 7.1: Đóng các bản ghi hiện hành nếu row_hash thay đổi
        conn.execute(text("""
            UPDATE gold.dim_customer d
            SET is_current = FALSE,
                valid_to = s.updated_at
            FROM silver.customers s
            WHERE s.batch_id = :batch_id
                AND d.customer_nk = s.customer_code
                AND d.is_current = TRUE
                AND d.row_hash <> s.row_hash;
        """), {"batch_id": batch_id})

        # Bước 7.2: Insert khách hàng mới hoàn toàn (valid_from = '1970-01-01' để cover toàn bộ lịch sử đơn cũ)
        # hoặc khách hàng vừa đổi hạng (valid_from = s.updated_at)
        conn.execute(text("""
            INSERT INTO gold.dim_customer (
                customer_nk, customer_name, customer_type, membership_tier,
                city, district, valid_from, valid_to, is_current, row_hash
            )
            SELECT
                s.customer_code,
                s.customer_name,
                s.customer_type,
                s.membership_tier,
                s.city,
                s.district,
                CASE
                    WHEN EXISTS (SELECT 1 FROM gold.dim_customer prev WHERE prev.customer_nk = s.customer_code)
                    THEN s.updated_at
                    ELSE TIMESTAMP '1970-01-01 00:00:00'
                END AS valid_from,
                TIMESTAMP '9999-12-31 23:59:59' AS valid_to,
                TRUE AS is_current,
                s.row_hash
            FROM silver.customers s
            LEFT JOIN gold.dim_customer d
                ON d.customer_nk = s.customer_code AND d.is_current = TRUE
            WHERE s.batch_id = :batch_id
                AND d.customer_sk IS NULL;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 8. NẠP DIM_COURIER (SCD TYPE 2 BẰNG ROW_HASH - PHỤ THUỘC DIM_HUB & DIM_VEHICLE)
        # =====================================================================
        conn.execute(text("""
            UPDATE gold.dim_courier d
            SET is_current = FALSE,
                valid_to = s.updated_at
            FROM silver.couriers s
            WHERE s.batch_id = :batch_id
                AND d.courier_nk = s.courier_code
                AND d.is_current = TRUE
                AND d.row_hash <> s.row_hash;
        """), {"batch_id": batch_id})

        conn.execute(text("""
            INSERT INTO gold.dim_courier (
                courier_nk, courier_name, hub_sk, contract_type,
                vehicle_sk, valid_from, valid_to, is_current, row_hash
            )
            SELECT
                s.courier_code,
                s.courier_name,
                COALESCE(dh.hub_sk, -1) AS hub_sk,
                s.contract_type,
                COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
                CASE
                    WHEN EXISTS (SELECT 1 FROM gold.dim_courier prev WHERE prev.courier_nk = s.courier_code)
                    THEN s.updated_at
                    ELSE TIMESTAMP '1970-01-01 00:00:00'
                END AS valid_from,
                TIMESTAMP '9999-12-31 23:59:59' AS valid_to,
                TRUE AS is_current,
                s.row_hash
            FROM silver.couriers s
            LEFT JOIN silver.hubs sh ON s.hub_id = sh.hub_id
            LEFT JOIN gold.dim_hub dh ON sh.hub_code = dh.hub_nk
            LEFT JOIN silver.vehicles sv ON s.vehicle_id = sv.vehicle_id
            LEFT JOIN gold.dim_vehicle dv ON sv.vehicle_code = dv.vehicle_nk
            LEFT JOIN gold.dim_courier d
                ON d.courier_nk = s.courier_code AND d.is_current = TRUE
            WHERE s.batch_id = :batch_id
                AND d.courier_sk IS NULL;
        """), {"batch_id": batch_id})

        conn.commit()
        print("✅ Đã nạp hoàn tất 8 bảng Dimensions!")