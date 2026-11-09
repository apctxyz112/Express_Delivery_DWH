{{ config(unique_key='event_id') }}

-- [AUDIT-B7] Không INNER JOIN silver_shipments để giữ các sự kiện của đơn đến muộn / đang chờ sửa Blocker
WITH src AS (
    SELECT
        e.event_id,
        e.shipment_id,
        e.event_time,
        TRIM(e.event_type) AS event_type,
        e.hub_id,
        e.courier_id,
        e.vehicle_id,
        e.status_reason_id,
        TRIM(e.scan_source) AS scan_source,
        e.updated_at,
        e.batch_id
    FROM {{ source('stg', 'shipment_tracking_events') }} e
    WHERE e.batch_id = '{{ var("batch_id") }}'
        AND NOT EXISTS (
            SELECT 1 FROM {{ source('meta', 'data_quarantine') }} q
            WHERE q.batch_id = '{{ var("batch_id") }}'
                AND q.source_table = 'shipment_tracking_events'
                AND q.severity = 'Blocker'
                AND q.record_id = TO_VARCHAR(e.event_id)
        )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY e.updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.event_id = tgt.event_id
WHERE tgt.event_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}