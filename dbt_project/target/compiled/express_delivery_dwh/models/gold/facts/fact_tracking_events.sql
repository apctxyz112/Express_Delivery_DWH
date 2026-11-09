

-- [AUDIT-D5, D8] Nạp lại toàn bộ sự kiện của các vận đơn bị ảnh hưởng để tính lại event_sequence chính xác
WITH impacted_shipments AS (
    SELECT shipment_id FROM EXPRESS_DWH.SILVER.silver_shipments
    
),
seq_events AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_time, e.event_id) AS event_seq
    FROM EXPRESS_DWH.SILVER.silver_shipment_tracking_events e
    JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
)
SELECT
    e.event_id AS tracking_event_sk,
    e.event_id AS event_id_nk,
    COALESCE(fsl.shipment_sk, -1) AS shipment_sk,
    ss.shipment_number,
    GOLD.FN_DATE_KEY(e.event_time) AS event_date_key,
    e.event_time AS event_timestamp,
    e.event_type,
    COALESCE(dh.hub_sk, -1) AS hub_sk,
    COALESCE(dco.courier_sk, -1) AS courier_sk,
    COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
    COALESCE(dsr.status_reason_sk, -1) AS status_reason_sk,
    e.event_seq AS event_sequence,
    e.scan_source,
    'AF_manual__2026-11-09T11:01:20.574764+00:00' AS batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS loaded_at
FROM seq_events e
JOIN EXPRESS_DWH.SILVER.silver_shipments ss ON e.shipment_id = ss.shipment_id
LEFT JOIN EXPRESS_DWH.GOLD.fact_shipment_lifecycle fsl ON ss.shipment_id = fsl.shipment_id_nk
LEFT JOIN EXPRESS_DWH.GOLD.dim_hub dh ON e.hub_id = dh.hub_sk
LEFT JOIN EXPRESS_DWH.SILVER.silver_couriers sco ON e.courier_id = sco.courier_id
LEFT JOIN EXPRESS_DWH.GOLD.dim_courier dco
    ON dco.courier_nk = sco.courier_code
    AND e.event_time >= dco.valid_from
    AND e.event_time < dco.valid_to
LEFT JOIN EXPRESS_DWH.GOLD.dim_vehicle dv ON e.vehicle_id = dv.vehicle_sk
LEFT JOIN EXPRESS_DWH.GOLD.dim_status_reason dsr ON e.status_reason_id = dsr.status_reason_sk
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY e.event_id
    ORDER BY dco.valid_from DESC NULLS LAST
) = 1