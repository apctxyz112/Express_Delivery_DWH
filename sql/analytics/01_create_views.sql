-- View 1: Tổng hợp SLA & OTD phục vụ Trang 1 Dashboard (Executive & SLA Overview)
CREATE OR REPLACE VIEW gold.v_slaship_summary AS
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
    COUNT(f.shipment_sk) AS total_shipments,
    SUM(f.freight_charge) AS total_freight_revenue,
    SUM(f.cod_amount) AS total_cod_amount,
    SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END) AS delivered_shipments,
    SUM(CASE WHEN f.is_returned THEN 1 ELSE 0 END) AS returned_shipments,
    SUM(CASE WHEN f.is_cancelled THEN 1 ELSE 0 END) AS cancelled_shipments,
    SUM(CASE WHEN f.is_delivered AND f.is_on_time THEN 1 ELSE 0 END) AS on_time_shipments,
    SUM(CASE WHEN f.is_sla_breached THEN 1 ELSE 0 END) AS sla_breached_shipments,
    ROUND(
        100.0 * SUM(CASE WHEN f.is_delivered AND f.is_on_time THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN f.is_delivered THEN 1 ELSE 0 END), 0),
        2
    ) AS otd_percentage,
    ROUND(AVG(f.total_lead_time_hours), 2) AS avg_total_lead_time_hours
FROM gold.fact_shipment_lifecycle f
JOIN gold.dim_date d ON f.order_date_key = d.date_key
JOIN gold.dim_service_type st ON f.service_type_sk = st.service_type_sk
JOIN gold.dim_hub ho ON f.origin_hub_sk = ho.hub_sk
JOIN gold.dim_hub hd ON f.destination_hub_sk = hd.hub_sk
JOIN gold.dim_customer cs ON f.sender_customer_sk = cs.customer_sk
GROUP BY
    d.full_date, d.year, d.month, d.month_name,
    st.service_name, st.is_express,
    ho.hub_name, ho.city,
    hd.hub_name, hd.city,
    cs.customer_type, cs.membership_tier;