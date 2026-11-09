{{
    config(
        materialized='incremental',
        unique_key='event_id_nk',
        incremental_strategy='merge',
        cluster_by=['event_date_key', 'hub_sk']
    )
}}

-- [AUDIT-D5, D8] Nạp lại toàn bộ sự kiện của các vận đơn bị ảnh hưởng để tính lại event_sequence chính xác
WITH impacted_shipments AS (
    SELECT shipment_id FROM {{ ref('silver_shipments') }}
    {% if is_incremental() %}
    WHERE batch_id = '{{ var("batch_id") }}'
    UNION
    SELECT DISTINCT shipment_id FROM {{ ref('silver_shipment_tracking_events') }} WHERE batch_id = '{{ var("batch_id") }}'
    UNION
    SELECT DISTINCT ss.shipment_id
    FROM {{ this }} f
    JOIN {{ ref('silver_shipments') }} ss ON f.shipment_number = ss.shipment_number
    JOIN {{ ref('dim_courier') }} c ON f.courier_sk = c.courier_sk
    WHERE c.is_current = FALSE
        AND NOT (f.event_timestamp >= c.valid_from AND f.event_timestamp < c.valid_to)
    {% endif %}
),
seq_events AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_time, e.event_id) AS event_seq
    FROM {{ ref('silver_shipment_tracking_events') }} e
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
    '{{ var("batch_id") }}' AS batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS loaded_at
FROM seq_events e
JOIN {{ ref('silver_shipments') }} ss ON e.shipment_id = ss.shipment_id
LEFT JOIN {{ ref('fact_shipment_lifecycle') }} fsl ON ss.shipment_id = fsl.shipment_id_nk
LEFT JOIN {{ ref('dim_hub') }} dh ON e.hub_id = dh.hub_sk
LEFT JOIN {{ ref('silver_couriers') }} sco ON e.courier_id = sco.courier_id
LEFT JOIN {{ ref('dim_courier') }} dco
    ON dco.courier_nk = sco.courier_code
    AND e.event_time >= dco.valid_from
    AND e.event_time < dco.valid_to
LEFT JOIN {{ ref('dim_vehicle') }} dv ON e.vehicle_id = dv.vehicle_sk
LEFT JOIN {{ ref('dim_status_reason') }} dsr ON e.status_reason_id = dsr.status_reason_sk
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY e.event_id
    ORDER BY dco.valid_from DESC NULLS LAST
) = 1