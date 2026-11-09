{{
    config(
        materialized='incremental',
        unique_key=['snapshot_date_key', 'hub_sk'],
        incremental_strategy='merge',
        cluster_by=['snapshot_date_key', 'hub_sk']
    )
}}

-- [AUDIT-D6, D7] Lưới ngày liên tục từ ngày sớm nhất có sự kiện thay đổi trong batch tới ngày hoạt động cuối
WITH affected_window AS (
    SELECT
        {% if is_incremental() %}
        COALESCE(
            (SELECT MIN(event_date_key) FROM {{ ref('fact_tracking_events') }} WHERE batch_id = '{{ var("batch_id") }}' AND event_date_key <> -1),
            (SELECT MAX(event_date_key) FROM {{ ref('fact_tracking_events') }} WHERE event_date_key <> -1)
        ) AS start_key,
        {% else %}
        (SELECT MIN(event_date_key) FROM {{ ref('fact_tracking_events') }} WHERE event_date_key <> -1) AS start_key,
        {% endif %}
        (SELECT MAX(event_date_key) FROM {{ ref('fact_tracking_events') }} WHERE event_date_key <> -1) AS end_key
),
snapshot_dates AS (
    SELECT d.date_key AS snapshot_date_key, d.full_date AS snapshot_date
    FROM affected_window w
    JOIN {{ ref('dim_date') }} d ON d.date_key BETWEEN w.start_key AND w.end_key
    WHERE d.date_key <> -1
),
date_hub_grid AS (
    SELECT sd.snapshot_date_key, sd.snapshot_date, h.hub_sk
    FROM snapshot_dates sd
    CROSS JOIN {{ ref('dim_hub') }} h
    WHERE h.hub_sk <> -1 AND h.is_current = TRUE
),
hub_daily_flow AS (
    SELECT
        e.event_date_key AS snapshot_date_key,
        e.hub_sk,
        COUNT_IF(e.event_type = 'INBOUND_SCAN') AS inbound_count,
        COUNT_IF(e.event_type = 'OUTBOUND_SCAN') AS outbound_count,
        COUNT_IF(e.event_type = 'RETURNED') AS return_count
    FROM {{ ref('fact_tracking_events') }} e
    JOIN snapshot_dates sd ON e.event_date_key = sd.snapshot_date_key
    WHERE e.hub_sk <> -1
    GROUP BY e.event_date_key, e.hub_sk
),
hub_backlog_shipments AS (
    SELECT DISTINCT
        g.snapshot_date_key,
        g.snapshot_date,
        g.hub_sk,
        in_ev.shipment_number,
        fsl.destination_hub_sk,
        fsl.cod_amount,
        fsl.cod_settled_timestamp
    FROM date_hub_grid g
    JOIN {{ ref('fact_tracking_events') }} in_ev
        ON in_ev.hub_sk = g.hub_sk
        AND in_ev.event_date_key <= g.snapshot_date_key
        AND in_ev.event_type = 'INBOUND_SCAN'
    JOIN {{ ref('fact_shipment_lifecycle') }} fsl
        ON in_ev.shipment_number = fsl.shipment_number
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('fact_tracking_events') }} out_ev
        WHERE out_ev.shipment_number = in_ev.shipment_number
            AND out_ev.event_date_key <= g.snapshot_date_key
            AND out_ev.event_timestamp >= in_ev.event_timestamp
            AND (
                (out_ev.hub_sk = in_ev.hub_sk AND out_ev.event_type = 'OUTBOUND_SCAN')
                OR out_ev.event_type IN ('DELIVERED', 'RETURNED')
                OR (out_ev.hub_sk <> -1 AND out_ev.hub_sk <> in_ev.hub_sk AND out_ev.event_type = 'INBOUND_SCAN')
            )
    )
),
hub_backlog_agg AS (
    SELECT
        b.snapshot_date_key,
        b.hub_sk,
        COUNT(b.shipment_number) AS backlog_count,
        -- [AUDIT-D7] Đơn đang tồn kho tại chính Hub đích cuối ngày = đơn chờ giao chặng cuối
        COUNT_IF(b.hub_sk = b.destination_hub_sk) AS pending_delivery_count,
        COALESCE(SUM(
            IFF(
                b.cod_amount > 0 AND (b.cod_settled_timestamp IS NULL OR b.cod_settled_timestamp::DATE > b.snapshot_date),
                b.cod_amount,
                0
            )
        ), 0) AS cod_held_amount
    FROM hub_backlog_shipments b
    GROUP BY b.snapshot_date_key, b.hub_sk
)
SELECT
    g.snapshot_date_key,
    g.hub_sk,
    COALESCE(f.inbound_count, 0) AS inbound_count,
    COALESCE(f.outbound_count, 0) AS outbound_count,
    COALESCE(b.backlog_count, 0) AS backlog_count,
    COALESCE(b.pending_delivery_count, 0) AS pending_delivery_count,
    COALESCE(f.return_count, 0) AS return_count,
    COALESCE(b.cod_held_amount, 0) AS cod_held_amount,
    '{{ var("batch_id") }}' AS batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS snapshot_generated_at
FROM date_hub_grid g
LEFT JOIN hub_daily_flow f
    ON g.snapshot_date_key = f.snapshot_date_key AND g.hub_sk = f.hub_sk
LEFT JOIN hub_backlog_agg b
    ON g.snapshot_date_key = b.snapshot_date_key AND g.hub_sk = b.hub_sk