

WITH ordered_hub_events AS (
    SELECT
        e.shipment_number,
        e.hub_sk,
        e.event_date_key,
        e.event_type,
        e.event_timestamp,
        LEAD(e.event_type) OVER (
            PARTITION BY e.shipment_number, e.hub_sk
            ORDER BY e.event_timestamp, e.event_sequence
        ) AS next_event_type,
        LEAD(e.event_timestamp) OVER (
            PARTITION BY e.shipment_number, e.hub_sk
            ORDER BY e.event_timestamp, e.event_sequence
        ) AS next_event_timestamp
    FROM EXPRESS_DWH.GOLD.fact_tracking_events e
    WHERE e.event_type IN ('INBOUND_SCAN', 'OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
        AND e.hub_sk <> -1
),
hub_dwell AS (
    SELECT
        hub_sk,
        event_date_key AS snapshot_date_key,
        AVG(DATEDIFF('second', event_timestamp, next_event_timestamp) / 3600.0) AS avg_dwell_time_hours
    FROM ordered_hub_events
    WHERE event_type = 'INBOUND_SCAN'
        AND next_event_type IN ('OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
        AND next_event_timestamp >= event_timestamp
    GROUP BY hub_sk, event_date_key
)
SELECT
    d.full_date AS snapshot_date,
    d.year,
    d.month,
    h.hub_nk AS hub_code,
    h.hub_name,
    h.hub_level,
    h.city,
    h.district,
    h.capacity,
    s.inbound_count,
    s.outbound_count,
    s.backlog_count,
    s.pending_delivery_count,
    s.return_count,
    s.cod_held_amount,
    ROUND(COALESCE(dw.avg_dwell_time_hours, 0), 2) AS avg_dwell_time_hours,
    ROUND(100.0 * s.backlog_count / NULLIF(h.capacity, 0), 2) AS backlog_capacity_ratio_pct
FROM EXPRESS_DWH.GOLD.fact_daily_hub_snapshot s
JOIN EXPRESS_DWH.GOLD.dim_date d ON s.snapshot_date_key = d.date_key
JOIN EXPRESS_DWH.GOLD.dim_hub h ON s.hub_sk = h.hub_sk
LEFT JOIN hub_dwell dw
    ON s.hub_sk = dw.hub_sk
    AND s.snapshot_date_key = dw.snapshot_date_key
WHERE h.hub_sk <> -1