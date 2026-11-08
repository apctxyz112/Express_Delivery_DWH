-- Materialized View 1: Hiệu năng & Tắc nghẽn Hub (Dwell Time + Backlog) cho Trang 2 Dashboard
DROP MATERIALIZED VIEW IF EXISTS gold.mv_daily_hub_performance;
CREATE MATERIALIZED VIEW gold.mv_daily_hub_performance AS
WITH hub_dwell AS (
    SELECT
        in_ev.hub_sk,
        in_ev.event_date_key AS snapshot_date_key,
        AVG(EXTRACT(EPOCH FROM (out_ev.event_timestamp - in_ev.event_timestamp)) / 3600.0) AS avg_dwell_time_hours
    FROM gold.fact_tracking_events in_ev
    JOIN gold.fact_tracking_events out_ev
      ON in_ev.shipment_sk = out_ev.shipment_sk
     AND in_ev.hub_sk = out_ev.hub_sk
     AND in_ev.event_type = 'INBOUND_SCAN'
     AND out_ev.event_type = 'OUTBOUND_SCAN'
     AND out_ev.event_timestamp >= in_ev.event_timestamp
    GROUP BY in_ev.hub_sk, in_ev.event_date_key
)
SELECT
    d.full_date AS snapshot_date,
    h.hub_nk AS hub_code,
    h.hub_name,
    h.hub_level,
    h.city,
    h.capacity,
    s.inbound_count,
    s.outbound_count,
    s.backlog_count,
    s.pending_delivery_count,
    s.return_count,
    s.cod_held_amount,
    ROUND(COALESCE(dw.avg_dwell_time_hours, 0)::numeric, 2) AS avg_dwell_time_hours,
    ROUND((100.0 * s.backlog_count / NULLIF(h.capacity, 0))::numeric, 2) AS backlog_capacity_ratio_pct
FROM gold.fact_daily_hub_snapshot s
JOIN gold.dim_date d ON s.snapshot_date_key = d.date_key
JOIN gold.dim_hub h ON s.hub_sk = h.hub_sk
LEFT JOIN hub_dwell dw ON s.hub_sk = dw.hub_sk AND s.snapshot_date_key = dw.snapshot_date_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hub_perf_unique
ON gold.mv_daily_hub_performance(snapshot_date, hub_code);

-- Materialized View 2: Hiệu suất Shipper & Phân tích Hoàn hàng (Trang 3 Dashboard - Chứng minh giá trị SCD2)
DROP MATERIALIZED VIEW IF EXISTS gold.mv_courier_performance;
CREATE MATERIALIZED VIEW gold.mv_courier_performance AS
SELECT
    d.year,
    d.month,
    c.courier_sk,
    c.courier_nk AS courier_code,
    c.courier_name,
    c.contract_type,
    c.is_current AS is_current_version,
    h.hub_name AS assigned_hub_at_event,
    COUNT(DISTINCT CASE WHEN e.event_type IN ('DELIVERED', 'RETURNED', 'DELIVERY_ATTEMPT') THEN e.shipment_sk END) AS total_handled_shipments,
    COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' THEN e.shipment_sk END) AS delivered_shipments,
    COUNT(DISTINCT CASE WHEN e.event_type = 'RETURNED' THEN e.shipment_sk END) AS returned_shipments,
    COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_sk END) AS on_time_delivered_shipments,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_sk END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' THEN e.shipment_sk END), 0),
        2
    ) AS courier_otd_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'RETURNED' THEN e.shipment_sk END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type IN ('DELIVERED', 'RETURNED') THEN e.shipment_sk END), 0),
        2
    ) AS courier_return_rate_pct
FROM gold.fact_tracking_events e
JOIN gold.fact_shipment_lifecycle f ON e.shipment_sk = f.shipment_sk
JOIN gold.dim_date d ON e.event_date_key = d.date_key
JOIN gold.dim_courier c ON e.courier_sk = c.courier_sk
JOIN gold.dim_hub h ON c.hub_sk = h.hub_sk
WHERE c.courier_sk <> -1
GROUP BY
    d.year, d.month,
    c.courier_sk, c.courier_nk, c.courier_name, c.contract_type, c.is_current,
    h.hub_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_courier_perf_unique
ON gold.mv_courier_performance(year, month, courier_sk);