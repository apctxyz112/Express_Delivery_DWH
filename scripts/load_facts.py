from sqlalchemy import text
from config.db_config import get_dwh_conn

def run_load_facts(batch_id: str):
    print(f"📊 [Batch: {batch_id}] Đang nạp 3 bảng Facts (SCD2 Point-in-Time Join & Idempotent Upsert)...")

    with get_dwh_conn() as conn:
        # =====================================================================
        # 1. NẠP FACT_SHIPMENT_LIFECYCLE (ACCUMULATING SNAPSHOT - UPSERT)
        # Join SCD2 với Dim_Customer theo đúng thời điểm nghiệp vụ order_date
        # FIX: Lấy status_reason từ sự kiện cuối cùng (latest event) thay vì map bừa
        # =====================================================================
        conn.execute(text("""
            WITH impacted_shipments AS (
                SELECT shipment_id FROM silver.shipments WHERE batch_id = :batch_id
                UNION
                SELECT DISTINCT shipment_id FROM silver.shipment_tracking_events WHERE batch_id = :batch_id
            ),
            event_milestones AS (
                SELECT
                    e.shipment_id,
                    MIN(CASE WHEN e.event_type = 'INBOUND_SCAN' THEN e.event_time END) AS first_sorting_time,
                    MAX(CASE WHEN e.event_type = 'OUTBOUND_SCAN' THEN e.event_time END) AS last_sorting_time,
                    MIN(CASE WHEN e.event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED', 'RETURNED') THEN e.event_time END) AS first_attempt_time,
                    MAX(CASE WHEN e.event_type = 'RETURNED' THEN e.event_time END) AS return_time
                FROM silver.shipment_tracking_events e
                JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
                GROUP BY e.shipment_id
            ),
            -- FIX LOGIC: Lấy reason_id của sự kiện cuối cùng (Latest Event) của từng shipment
            latest_event_reasons AS (
                SELECT DISTINCT ON (e.shipment_id)
                    e.shipment_id, e.status_reason_id
                FROM silver.shipment_tracking_events e
                JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
                ORDER BY e.shipment_id, e.event_time DESC, e.event_id DESC
            ),
            -- Map reason_id từ Silver sang Surrogate Key ở Gold
            status_lookup AS (
                SELECT sr.status_reason_id, dsr.status_reason_sk
                FROM silver.status_reasons sr
                JOIN gold.dim_status_reason dsr ON sr.reason_code = dsr.reason_code
            )
            INSERT INTO gold.fact_shipment_lifecycle (
                shipment_number,
                sender_customer_sk, receiver_customer_sk,
                origin_hub_sk, destination_hub_sk,
                service_type_sk, payment_method_sk, current_status_sk,
                order_date_key, pickup_date_key, first_sorting_date_key, last_sorting_date_key,
                first_delivery_attempt_date_key, final_delivery_date_key, return_date_key, promised_delivery_date_key,
                weight, freight_charge, cod_amount, distance_km,
                pickup_to_sorting_hours, sorting_to_delivery_hours, total_lead_time_hours,
                is_delivered, is_returned, is_cancelled, is_on_time, is_sla_breached,
                created_at, updated_at, batch_id
            )
            SELECT
                s.shipment_number,
                COALESCE(dc_send.customer_sk, -1) AS sender_customer_sk,
                COALESCE(dc_recv.customer_sk, -1) AS receiver_customer_sk,
                COALESCE(dh_orig.hub_sk, -1) AS origin_hub_sk,
                COALESCE(dh_dest.hub_sk, -1) AS destination_hub_sk,
                COALESCE(dst.service_type_sk, -1) AS service_type_sk,
                COALESCE(dpm.payment_method_sk, -1) AS payment_method_sk,
                COALESCE(dsl.status_reason_sk, -1) AS current_status_sk,

                COALESCE(TO_CHAR(s.order_date, 'YYYYMMDD')::INT, -1) AS order_date_key,
                COALESCE(TO_CHAR(s.pickup_date, 'YYYYMMDD')::INT, -1) AS pickup_date_key,
                COALESCE(TO_CHAR(m.first_sorting_time, 'YYYYMMDD')::INT, -1) AS first_sorting_date_key,
                COALESCE(TO_CHAR(m.last_sorting_time, 'YYYYMMDD')::INT, -1) AS last_sorting_date_key,
                COALESCE(TO_CHAR(m.first_attempt_time, 'YYYYMMDD')::INT, -1) AS first_delivery_attempt_date_key,
                CASE WHEN s.current_status = 'DELIVERED' THEN COALESCE(TO_CHAR(s.actual_delivery_date, 'YYYYMMDD')::INT, -1) ELSE -1 END AS final_delivery_date_key,
                CASE WHEN s.current_status = 'RETURNED' THEN COALESCE(TO_CHAR(COALESCE(m.return_time, s.actual_delivery_date), 'YYYYMMDD')::INT, -1) ELSE -1 END AS return_date_key,
                COALESCE(TO_CHAR(s.promised_delivery_date, 'YYYYMMDD')::INT, -1) AS promised_delivery_date_key,

                s.weight,
                s.freight_charge,
                s.cod_amount,
                s.distance_km,

                ROUND((EXTRACT(EPOCH FROM (m.first_sorting_time - s.pickup_date)) / 3600.0)::NUMERIC, 2) AS pickup_to_sorting_hours,
                ROUND((EXTRACT(EPOCH FROM (s.actual_delivery_date - m.first_sorting_time)) / 3600.0)::NUMERIC, 2) AS sorting_to_delivery_hours,
                ROUND((EXTRACT(EPOCH FROM (s.actual_delivery_date - s.order_date)) / 3600.0)::NUMERIC, 2) AS total_lead_time_hours,

                (s.current_status = 'DELIVERED') AS is_delivered,
                (s.current_status = 'RETURNED') AS is_returned,
                (s.current_status = 'CANCELLED') AS is_cancelled,
                CASE
                    WHEN s.current_status = 'DELIVERED' AND s.actual_delivery_date <= s.promised_delivery_date THEN TRUE
                    ELSE FALSE
                END AS is_on_time,
                CASE
                    WHEN s.actual_delivery_date IS NOT NULL AND s.actual_delivery_date > s.promised_delivery_date THEN TRUE
                    ELSE FALSE
                END AS is_sla_breached,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                :batch_id
            FROM silver.shipments s
            JOIN impacted_shipments imp ON s.shipment_id = imp.shipment_id
            LEFT JOIN event_milestones m ON s.shipment_id = m.shipment_id
            LEFT JOIN silver.customers sc_send ON s.sender_customer_id = sc_send.customer_id
            -- Join SCD2 theo đúng thời điểm đặt hàng (Point-in-Time Join)
            LEFT JOIN LATERAL (
                SELECT customer_sk FROM gold.dim_customer c
                WHERE c.customer_nk = sc_send.customer_code
                    AND s.order_date >= c.valid_from AND s.order_date < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dc_send ON TRUE
            LEFT JOIN silver.customers sc_recv ON s.receiver_customer_id = sc_recv.customer_id
            LEFT JOIN LATERAL (
                SELECT customer_sk FROM gold.dim_customer c
                WHERE c.customer_nk = sc_recv.customer_code
                    AND s.order_date >= c.valid_from AND s.order_date < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dc_recv ON TRUE
            LEFT JOIN silver.hubs sh_orig ON s.origin_hub_id = sh_orig.hub_id
            LEFT JOIN gold.dim_hub dh_orig ON sh_orig.hub_code = dh_orig.hub_nk
            LEFT JOIN silver.hubs sh_dest ON s.destination_hub_id = sh_dest.hub_id
            LEFT JOIN gold.dim_hub dh_dest ON sh_dest.hub_code = dh_dest.hub_nk
            LEFT JOIN silver.service_types sst ON s.service_type_id = sst.service_type_id
            LEFT JOIN gold.dim_service_type dst ON sst.service_code = dst.service_type_nk
            LEFT JOIN silver.payment_methods spm ON s.payment_method_id = spm.payment_method_id
            LEFT JOIN gold.dim_payment_method dpm ON spm.payment_method_code = dpm.payment_method_nk
            -- FIX LOGIC: Join với sự kiện cuối cùng để lấy đúng lý do trạng thái
            LEFT JOIN latest_event_reasons ler ON s.shipment_id = ler.shipment_id
            LEFT JOIN status_lookup dsl ON ler.status_reason_id = dsl.status_reason_id
            ON CONFLICT (shipment_number) DO UPDATE SET
                current_status_sk = EXCLUDED.current_status_sk,
                pickup_date_key = EXCLUDED.pickup_date_key,
                first_sorting_date_key = EXCLUDED.first_sorting_date_key,
                last_sorting_date_key = EXCLUDED.last_sorting_date_key,
                first_delivery_attempt_date_key = EXCLUDED.first_delivery_attempt_date_key,
                final_delivery_date_key = EXCLUDED.final_delivery_date_key,
                return_date_key = EXCLUDED.return_date_key,
                pickup_to_sorting_hours = EXCLUDED.pickup_to_sorting_hours,
                sorting_to_delivery_hours = EXCLUDED.sorting_to_delivery_hours,
                total_lead_time_hours = EXCLUDED.total_lead_time_hours,
                is_delivered = EXCLUDED.is_delivered,
                is_returned = EXCLUDED.is_returned,
                is_cancelled = EXCLUDED.is_cancelled,
                is_on_time = EXCLUDED.is_on_time,
                is_sla_breached = EXCLUDED.is_sla_breached,
                updated_at = CURRENT_TIMESTAMP,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 2. NẠP FACT_TRACKING_EVENTS (TRANSACTION FACT - UPSERT THEO EVENT_ID_NK)
        # Join SCD2 với Dim_Courier theo đúng thời điểm quét event_time
        # =====================================================================
        conn.execute(text("""
            WITH seq_events AS (
                SELECT
                    e.*,
                    ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_time, e.event_id) AS event_seq
                FROM silver.shipment_tracking_events e
                WHERE e.shipment_id IN (
                    SELECT DISTINCT shipment_id FROM silver.shipment_tracking_events WHERE batch_id = :batch_id
                )
            )
            INSERT INTO gold.fact_tracking_events (
                event_id_nk, shipment_sk, event_date_key, event_timestamp,
                event_type, hub_sk, courier_sk, vehicle_sk, status_reason_sk,
                event_sequence, scan_source, batch_id, loaded_at
            )
            SELECT
                e.event_id AS event_id_nk,
                fsl.shipment_sk,
                COALESCE(TO_CHAR(e.event_time, 'YYYYMMDD')::INT, -1) AS event_date_key,
                e.event_time AS event_timestamp,
                e.event_type,
                COALESCE(dh.hub_sk, -1) AS hub_sk,
                COALESCE(dco.courier_sk, -1) AS courier_sk,
                COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
                COALESCE(dsr.status_reason_sk, -1) AS status_reason_sk,
                e.event_seq AS event_sequence,
                e.scan_source,
                :batch_id,
                CURRENT_TIMESTAMP
            FROM seq_events e
            JOIN silver.shipments ss ON e.shipment_id = ss.shipment_id
            JOIN gold.fact_shipment_lifecycle fsl ON ss.shipment_number = fsl.shipment_number
            LEFT JOIN silver.hubs sh ON e.hub_id = sh.hub_id
            LEFT JOIN gold.dim_hub dh ON sh.hub_code = dh.hub_nk
            LEFT JOIN silver.couriers sco ON e.courier_id = sco.courier_id
            -- Join SCD2 Dim_Courier theo thời điểm phát sinh sự kiện (event_time)
            LEFT JOIN LATERAL (
                SELECT courier_sk FROM gold.dim_courier c
                WHERE c.courier_nk = sco.courier_code
                    AND e.event_time >= c.valid_from AND e.event_time < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dco ON TRUE
            LEFT JOIN silver.vehicles sv ON e.vehicle_id = sv.vehicle_id
            LEFT JOIN gold.dim_vehicle dv ON sv.vehicle_code = dv.vehicle_nk
            LEFT JOIN silver.status_reasons ssr ON e.status_reason_id = ssr.status_reason_id
            LEFT JOIN gold.dim_status_reason dsr ON ssr.reason_code = dsr.reason_code
            WHERE e.batch_id = :batch_id
            ON CONFLICT (event_id_nk) DO UPDATE SET
                event_date_key = EXCLUDED.event_date_key,
                event_timestamp = EXCLUDED.event_timestamp,
                event_type = EXCLUDED.event_type,
                hub_sk = EXCLUDED.hub_sk,
                courier_sk = EXCLUDED.courier_sk,
                vehicle_sk = EXCLUDED.vehicle_sk,
                status_reason_sk = EXCLUDED.status_reason_sk,
                event_sequence = EXCLUDED.event_sequence,
                batch_id = EXCLUDED.batch_id,
                loaded_at = CURRENT_TIMESTAMP;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 3. NẠP FACT_DAILY_HUB_SNAPSHOT (PERIODIC SNAPSHOT - IDEMPOTENT UPSERT)
        # =====================================================================
        conn.execute(text("""
            WITH active_dates AS (
                SELECT DISTINCT event_date_key AS snapshot_date_key
                FROM gold.fact_tracking_events
                WHERE batch_id = :batch_id AND event_date_key <> -1
            ),
            hub_daily_flow AS (
                SELECT
                    e.event_date_key AS snapshot_date_key,
                    e.hub_sk,
                    COUNT(CASE WHEN e.event_type = 'INBOUND_SCAN' THEN 1 END) AS inbound_count,
                    COUNT(CASE WHEN e.event_type = 'OUTBOUND_SCAN' THEN 1 END) AS outbound_count,
                    COUNT(CASE WHEN e.event_type = 'DELIVERY_ATTEMPT' THEN 1 END) AS pending_delivery_count,
                    COUNT(CASE WHEN e.event_type = 'RETURNED' THEN 1 END) AS return_count
                FROM gold.fact_tracking_events e
                JOIN active_dates ad ON e.event_date_key = ad.snapshot_date_key
                WHERE e.hub_sk <> -1
                GROUP BY e.event_date_key, e.hub_sk
            ),
            hub_backlog AS (
                SELECT
                    ad.snapshot_date_key,
                    in_ev.hub_sk,
                    COUNT(DISTINCT in_ev.shipment_sk) AS backlog_count,
                    COALESCE(SUM(DISTINCT CASE WHEN cs.settlement_status = 'PENDING' THEN fsl.cod_amount ELSE 0 END), 0) AS cod_held_amount
                FROM active_dates ad
                JOIN gold.fact_tracking_events in_ev
                    ON in_ev.event_date_key <= ad.snapshot_date_key
                    AND in_ev.event_type = 'INBOUND_SCAN'
                    AND in_ev.hub_sk <> -1
                JOIN gold.fact_shipment_lifecycle fsl ON in_ev.shipment_sk = fsl.shipment_sk
                LEFT JOIN silver.shipments ss ON fsl.shipment_number = ss.shipment_number
                LEFT JOIN silver.cod_settlements cs ON ss.shipment_id = cs.shipment_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM gold.fact_tracking_events out_ev
                    WHERE out_ev.shipment_sk = in_ev.shipment_sk
                        AND out_ev.hub_sk = in_ev.hub_sk
                        AND out_ev.event_type IN ('OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
                        AND out_ev.event_date_key <= ad.snapshot_date_key
                        AND out_ev.event_timestamp >= in_ev.event_timestamp
                )
                GROUP BY ad.snapshot_date_key, in_ev.hub_sk
            )
            INSERT INTO gold.fact_daily_hub_snapshot (
                snapshot_date_key, hub_sk,
                inbound_count, outbound_count, backlog_count,
                pending_delivery_count, return_count, cod_held_amount,
                batch_id, snapshot_generated_at
            )
            SELECT
                COALESCE(f.snapshot_date_key, b.snapshot_date_key) AS snapshot_date_key,
                COALESCE(f.hub_sk, b.hub_sk) AS hub_sk,
                COALESCE(f.inbound_count, 0) AS inbound_count,
                COALESCE(f.outbound_count, 0) AS outbound_count,
                COALESCE(b.backlog_count, 0) AS backlog_count,
                COALESCE(f.pending_delivery_count, 0) AS pending_delivery_count,
                COALESCE(f.return_count, 0) AS return_count,
                COALESCE(b.cod_held_amount, 0) AS cod_held_amount,
                :batch_id,
                CURRENT_TIMESTAMP
            FROM hub_daily_flow f
            FULL OUTER JOIN hub_backlog b
                ON f.snapshot_date_key = b.snapshot_date_key AND f.hub_sk = b.hub_sk
            ON CONFLICT (snapshot_date_key, hub_sk) DO UPDATE SET
                inbound_count = EXCLUDED.inbound_count,
                outbound_count = EXCLUDED.outbound_count,
                backlog_count = EXCLUDED.backlog_count,
                pending_delivery_count = EXCLUDED.pending_delivery_count,
                return_count = EXCLUDED.return_count,
                cod_held_amount = EXCLUDED.cod_held_amount,
                batch_id = EXCLUDED.batch_id,
                snapshot_generated_at = CURRENT_TIMESTAMP;
        """), {"batch_id": batch_id})

        conn.commit()
        print("✅ Đã nạp hoàn tất 3 bảng Facts!")