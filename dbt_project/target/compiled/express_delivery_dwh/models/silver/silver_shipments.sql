

WITH src AS (
    SELECT
        s.shipment_id,
        TRIM(s.shipment_number) AS shipment_number,
        s.sender_customer_id,
        s.receiver_customer_id, -- [AUDIT-B3] Cho phép NULL, map về SK = -1 ở Gold
        s.origin_hub_id,
        s.destination_hub_id,
        s.service_type_id,
        s.payment_method_id,
        s.weight,
        COALESCE(s.distance_km, 10.0) AS distance_km,
        s.freight_charge,
        COALESCE(s.cod_amount, 0) AS cod_amount,
        s.order_date,
        s.pickup_date,
        s.promised_delivery_date,
        s.actual_delivery_date,
        TRIM(s.current_status) AS current_status,
        s.updated_at,
        s.batch_id
    FROM EXPRESS_DWH.STG.shipments s
    WHERE s.batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
        AND NOT EXISTS (
            SELECT 1 FROM EXPRESS_DWH.META.data_quarantine q
            WHERE q.batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
                AND q.source_table = 'shipments'
                AND q.severity = 'Blocker'
                AND q.record_id = TO_VARCHAR(s.shipment_id)
        )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.shipment_id ORDER BY s.updated_at DESC) = 1
)
SELECT s.*
FROM src s
