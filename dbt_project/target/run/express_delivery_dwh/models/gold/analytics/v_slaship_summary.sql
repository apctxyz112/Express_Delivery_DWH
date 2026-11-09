
  create or replace   view EXPRESS_DWH.GOLD.v_slaship_summary
  
   as (
    

SELECT
    d.full_date AS order_date,
    d.year,
    d.month,
    d.month_name,
    st.service_name,
    st.is_express,
    ho.hub_name AS origin_hub,
    ho.city AS origin_city,
    hd.hub_name AS destination_hub,
    hd.city AS destination_city,
    cs.customer_type AS sender_type,
    cs.membership_tier AS sender_tier,
    sr.status_code AS current_status_code,
    sr.reason_name AS latest_status_reason,
    COUNT(f.shipment_sk) AS total_shipments,
    SUM(f.freight_charge) AS total_freight_revenue,
    SUM(f.cod_amount) AS total_cod_amount,
    SUM(IFF(f.is_cod_settled, f.cod_amount, 0)) AS settled_cod_amount,
    COUNT_IF(f.is_delivered) AS delivered_shipments,
    COUNT_IF(f.is_returned) AS returned_shipments,
    COUNT_IF(f.is_cancelled) AS cancelled_shipments,
    COUNT_IF(f.is_delivered AND f.is_on_time) AS on_time_shipments,
    COUNT_IF(f.is_sla_breached) AS sla_breached_shipments,
    -- [AUDIT-V1] Đơn chưa kết thúc nhưng đã quá hạn hẹn giao tại thời điểm truy vấn
    COUNT_IF(
        NOT (f.is_delivered OR f.is_returned OR f.is_cancelled)
        AND f.promised_delivery_timestamp < CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
    ) AS overdue_open_shipments,
    SUM(f.ticket_count) AS total_complaint_tickets,
    ROUND(
        100.0 * COUNT_IF(f.is_delivered AND f.is_on_time)
        / NULLIF(COUNT_IF(f.is_delivered), 0),
        2
    ) AS otd_percentage,
    ROUND(AVG(f.total_lead_time_hours), 2) AS avg_total_lead_time_hours
FROM EXPRESS_DWH.GOLD.fact_shipment_lifecycle f
JOIN EXPRESS_DWH.GOLD.dim_date d ON f.order_date_key = d.date_key
JOIN EXPRESS_DWH.GOLD.dim_service_type st ON f.service_type_sk = st.service_type_sk
JOIN EXPRESS_DWH.GOLD.dim_hub ho ON f.origin_hub_sk = ho.hub_sk
JOIN EXPRESS_DWH.GOLD.dim_hub hd ON f.destination_hub_sk = hd.hub_sk
JOIN EXPRESS_DWH.GOLD.dim_customer cs ON f.sender_customer_sk = cs.customer_sk
JOIN EXPRESS_DWH.GOLD.dim_status_reason sr ON f.current_status_sk = sr.status_reason_sk
GROUP BY
    d.full_date, d.year, d.month, d.month_name,
    st.service_name, st.is_express,
    ho.hub_name, ho.city,
    hd.hub_name, hd.city,
    cs.customer_type, cs.membership_tier,
    sr.status_code, sr.reason_name
  );

