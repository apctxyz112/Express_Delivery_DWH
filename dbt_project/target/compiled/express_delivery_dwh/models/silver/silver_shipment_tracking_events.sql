

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
    FROM EXPRESS_DWH.STG.shipment_tracking_events e
    WHERE e.batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
        AND NOT EXISTS (
            SELECT 1 FROM EXPRESS_DWH.META.data_quarantine q
            WHERE q.batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
                AND q.source_table = 'shipment_tracking_events'
                AND q.severity = 'Blocker'
                AND q.record_id = TO_VARCHAR(e.event_id)
        )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY e.updated_at DESC) = 1
)
SELECT s.*
FROM src s
