

-- [AUDIT-D3, D8] Tập vận đơn cần tính lại: có thay đổi ở bất kỳ bảng nào trong 4 bảng Silver HOẶC cần re-key SCD2
WITH impacted_shipments AS (
    SELECT shipment_id FROM EXPRESS_DWH.SILVER.silver_shipments
    
),
event_milestones AS (
    SELECT
        e.shipment_id,
        MIN(CASE WHEN e.event_type = 'INBOUND_SCAN' THEN e.event_time END) AS first_sorting_time,
        MAX(CASE WHEN e.event_type = 'OUTBOUND_SCAN' THEN e.event_time END) AS last_sorting_time,
        -- [AUDIT-D10] Chỉ đếm DELIVERY_ATTEMPT và DELIVERED là lần phát
        MIN(CASE WHEN e.event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED') THEN e.event_time END) AS first_attempt_time,
        MAX(CASE WHEN e.event_type = 'RETURNED' THEN e.event_time END) AS return_time,
        COUNT(CASE WHEN e.event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED') THEN 1 END) AS delivery_attempt_count
    FROM EXPRESS_DWH.SILVER.silver_shipment_tracking_events e
    JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
    GROUP BY e.shipment_id
),
latest_event_reason AS (
    SELECT
        e.shipment_id,
        dsr.status_reason_sk,
        dsr.status_code
    FROM EXPRESS_DWH.SILVER.silver_shipment_tracking_events e
    JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
    LEFT JOIN EXPRESS_DWH.GOLD.dim_status_reason dsr ON e.status_reason_id = dsr.status_reason_sk
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_time DESC, e.event_id DESC) = 1
),
fallback_status_lookup AS (
    SELECT status_code, status_reason_sk
    FROM EXPRESS_DWH.GOLD.dim_status_reason
    WHERE status_reason_sk <> -1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status_code ORDER BY status_reason_sk) = 1
),
cod_agg AS (
    SELECT
        c.shipment_id,
        c.settlement_status,
        c.settled_at
    FROM EXPRESS_DWH.SILVER.silver_cod_settlements c
    JOIN impacted_shipments imp ON c.shipment_id = imp.shipment_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.shipment_id ORDER BY c.updated_at DESC) = 1
),
ticket_agg AS (
    SELECT
        t.shipment_id,
        COUNT(t.ticket_id) AS ticket_count
    FROM EXPRESS_DWH.SILVER.silver_customer_tickets t
    JOIN impacted_shipments imp ON t.shipment_id = imp.shipment_id
    GROUP BY t.shipment_id
)
SELECT
    s.shipment_id AS shipment_sk,
    s.shipment_id AS shipment_id_nk,
    s.shipment_number,
    COALESCE(dc_send.customer_sk, -1) AS sender_customer_sk,
    COALESCE(dc_recv.customer_sk, -1) AS receiver_customer_sk,
    COALESCE(dh_orig.hub_sk, -1) AS origin_hub_sk,
    COALESCE(dh_dest.hub_sk, -1) AS destination_hub_sk,
    COALESCE(dst.service_type_sk, -1) AS service_type_sk,
    COALESCE(dpm.payment_method_sk, -1) AS payment_method_sk,
    -- [AUDIT-D1] Chỉ lấy lý do của sự kiện cuối khi status_code khớp với trạng thái hiện tại của vận đơn
    COALESCE(
        CASE WHEN ler.status_code = s.current_status THEN ler.status_reason_sk END,
        fsl_st.status_reason_sk,
        -1
    ) AS current_status_sk,

    -- Role-playing Date Keys qua hàm an toàn GOLD.FN_DATE_KEY [AUDIT-D4]
    GOLD.FN_DATE_KEY(s.order_date) AS order_date_key,
    GOLD.FN_DATE_KEY(s.pickup_date) AS pickup_date_key,
    GOLD.FN_DATE_KEY(m.first_sorting_time) AS first_sorting_date_key,
    GOLD.FN_DATE_KEY(m.last_sorting_time) AS last_sorting_date_key,
    GOLD.FN_DATE_KEY(m.first_attempt_time) AS first_delivery_attempt_date_key,
    -- [AUDIT-D2] Chỉ đơn DELIVERED mới có final_delivery_date_key
    IFF(s.current_status = 'DELIVERED', GOLD.FN_DATE_KEY(s.actual_delivery_date), -1) AS final_delivery_date_key,
    IFF(s.current_status = 'RETURNED', GOLD.FN_DATE_KEY(COALESCE(m.return_time, s.actual_delivery_date)), -1) AS return_date_key,
    GOLD.FN_DATE_KEY(s.promised_delivery_date) AS promised_delivery_date_key,
    IFF(ca.settlement_status = 'SETTLED', GOLD.FN_DATE_KEY(ca.settled_at), -1) AS cod_settled_date_key,

    -- Milestone Timestamps
    s.order_date AS order_timestamp,
    s.pickup_date AS pickup_timestamp,
    m.first_sorting_time AS first_sorting_timestamp,
    m.last_sorting_time AS last_sorting_timestamp,
    m.first_attempt_time AS first_delivery_attempt_timestamp,
    IFF(s.current_status = 'DELIVERED', s.actual_delivery_date, NULL) AS final_delivery_timestamp,
    s.promised_delivery_date AS promised_delivery_timestamp,
    IFF(ca.settlement_status = 'SETTLED', ca.settled_at, NULL) AS cod_settled_timestamp,

    -- Additive Measures
    s.weight,
    s.freight_charge,
    s.cod_amount,
    s.distance_km,
    COALESCE(m.delivery_attempt_count, 0) AS delivery_attempt_count,
    COALESCE(ta.ticket_count, 0) AS ticket_count,

    -- Lead-time Measures (Hours) - Dùng DATEDIFF chuẩn Snowflake
    ROUND(DATEDIFF('second', s.pickup_date, m.first_sorting_time) / 3600.0, 2) AS pickup_to_sorting_hours,
    IFF(
        s.current_status = 'DELIVERED',
        ROUND(DATEDIFF('second', m.first_sorting_time, s.actual_delivery_date) / 3600.0, 2),
        NULL
    ) AS sorting_to_delivery_hours,
    IFF(
        s.current_status = 'DELIVERED',
        ROUND(DATEDIFF('second', s.order_date, s.actual_delivery_date) / 3600.0, 2),
        NULL
    ) AS total_lead_time_hours,

    -- Operational & SLA Flags
    (s.current_status = 'DELIVERED') AS is_delivered,
    (s.current_status = 'RETURNED') AS is_returned,
    (s.current_status = 'CANCELLED') AS is_cancelled,
    IFF(s.current_status = 'DELIVERED' AND s.actual_delivery_date <= s.promised_delivery_date, TRUE, FALSE) AS is_on_time,
    IFF(s.current_status = 'DELIVERED' AND s.actual_delivery_date > s.promised_delivery_date, TRUE, FALSE) AS is_sla_breached,
    (COALESCE(ca.settlement_status, 'NONE') = 'SETTLED') AS is_cod_settled,
    (COALESCE(ta.ticket_count, 0) > 0) AS has_complaint_ticket,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS updated_at,
    'AF_manual__2026-11-09T11:01:20.574764+00:00' AS batch_id
FROM EXPRESS_DWH.SILVER.silver_shipments s
JOIN impacted_shipments imp ON s.shipment_id = imp.shipment_id
LEFT JOIN event_milestones m ON s.shipment_id = m.shipment_id
LEFT JOIN latest_event_reason ler ON s.shipment_id = ler.shipment_id
LEFT JOIN fallback_status_lookup fsl_st ON s.current_status = fsl_st.status_code
LEFT JOIN cod_agg ca ON s.shipment_id = ca.shipment_id
LEFT JOIN ticket_agg ta ON s.shipment_id = ta.shipment_id
LEFT JOIN EXPRESS_DWH.SILVER.silver_customers sc_send ON s.sender_customer_id = sc_send.customer_id
LEFT JOIN EXPRESS_DWH.GOLD.dim_customer dc_send
    ON dc_send.customer_nk = sc_send.customer_code
    AND s.order_date >= dc_send.valid_from
    AND s.order_date < dc_send.valid_to
LEFT JOIN EXPRESS_DWH.SILVER.silver_customers sc_recv ON s.receiver_customer_id = sc_recv.customer_id
LEFT JOIN EXPRESS_DWH.GOLD.dim_customer dc_recv
    ON dc_recv.customer_nk = sc_recv.customer_code
    AND s.order_date >= dc_recv.valid_from
    AND s.order_date < dc_recv.valid_to
LEFT JOIN EXPRESS_DWH.GOLD.dim_hub dh_orig ON s.origin_hub_id = dh_orig.hub_sk
LEFT JOIN EXPRESS_DWH.GOLD.dim_hub dh_dest ON s.destination_hub_id = dh_dest.hub_sk
LEFT JOIN EXPRESS_DWH.GOLD.dim_service_type dst ON s.service_type_id = dst.service_type_sk
LEFT JOIN EXPRESS_DWH.GOLD.dim_payment_method dpm ON s.payment_method_id = dpm.payment_method_sk
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.shipment_id
    ORDER BY dc_send.valid_from DESC NULLS LAST, dc_recv.valid_from DESC NULLS LAST
) = 1