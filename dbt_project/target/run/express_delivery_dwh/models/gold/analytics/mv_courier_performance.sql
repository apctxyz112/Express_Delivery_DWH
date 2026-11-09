
  
    

        create or replace transient table EXPRESS_DWH.GOLD.mv_courier_performance
         as
        (

SELECT
    d.year,
    d.month,
    c.courier_sk,
    c.courier_nk AS courier_code,
    c.courier_name,
    c.contract_type,
    c.valid_from AS version_valid_from,
    c.valid_to AS version_valid_to,
    c.is_current AS is_current_version,
    c.assigned_hub_code AS assigned_hub_code_at_event,
    c.assigned_hub_name AS assigned_hub_at_event,
    c.assigned_hub_city AS assigned_hub_city,
    c.assigned_vehicle_type AS assigned_vehicle_type,
    COUNT(DISTINCT CASE
        WHEN e.event_type IN ('DELIVERED', 'RETURNED', 'DELIVERY_ATTEMPT') THEN e.shipment_number
    END) AS total_handled_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'PICKED_UP' THEN e.shipment_number
    END) AS total_picked_up_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'DELIVERED' THEN e.shipment_number
    END) AS delivered_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'RETURNED' THEN e.shipment_number
    END) AS returned_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_number
    END) AS on_time_delivered_shipments,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_number END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' THEN e.shipment_number END), 0),
        2
    ) AS courier_otd_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'RETURNED' THEN e.shipment_number END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type IN ('DELIVERED', 'RETURNED') THEN e.shipment_number END), 0),
        2
    ) AS courier_return_rate_pct
FROM EXPRESS_DWH.GOLD.fact_tracking_events e
JOIN EXPRESS_DWH.GOLD.fact_shipment_lifecycle f ON e.shipment_number = f.shipment_number
JOIN EXPRESS_DWH.GOLD.dim_date d ON e.event_date_key = d.date_key
JOIN EXPRESS_DWH.GOLD.dim_courier c ON e.courier_sk = c.courier_sk
WHERE c.courier_sk <> -1
GROUP BY
    d.year, d.month,
    c.courier_sk, c.courier_nk, c.courier_name, c.contract_type,
    c.valid_from, c.valid_to, c.is_current,
    c.assigned_hub_code, c.assigned_hub_name, c.assigned_hub_city, c.assigned_vehicle_type
        );
      
  