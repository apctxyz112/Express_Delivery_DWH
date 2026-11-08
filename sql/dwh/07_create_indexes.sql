-- Indexes cho Dimensions (SCD2 & Natural Keys)
CREATE INDEX IF NOT EXISTS idx_dim_cust_nk_valid ON gold.dim_customer(customer_nk, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_dim_cust_current ON gold.dim_customer(customer_nk, is_current);
CREATE INDEX IF NOT EXISTS idx_dim_cour_nk_valid ON gold.dim_courier(courier_nk, valid_from, valid_to);
CREATE INDEX IF NOT EXISTS idx_dim_cour_current ON gold.dim_courier(courier_nk, is_current);

-- Indexes cho Fact_Tracking_Events
CREATE INDEX IF NOT EXISTS idx_fte_event_date ON gold.fact_tracking_events(event_date_key);
CREATE INDEX IF NOT EXISTS idx_fte_hub_date ON gold.fact_tracking_events(hub_sk, event_date_key);
CREATE INDEX IF NOT EXISTS idx_fte_courier_date ON gold.fact_tracking_events(courier_sk, event_date_key);
CREATE INDEX IF NOT EXISTS idx_fte_shipment ON gold.fact_tracking_events(shipment_sk);

-- Indexes cho Fact_Shipment_Lifecycle
CREATE INDEX IF NOT EXISTS idx_fsl_order_date ON gold.fact_shipment_lifecycle(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fsl_deliv_date ON gold.fact_shipment_lifecycle(final_delivery_date_key);
CREATE INDEX IF NOT EXISTS idx_fsl_hubs ON gold.fact_shipment_lifecycle(origin_hub_sk, destination_hub_sk);
CREATE INDEX IF NOT EXISTS idx_fsl_service ON gold.fact_shipment_lifecycle(service_type_sk);
CREATE INDEX IF NOT EXISTS idx_fsl_status ON gold.fact_shipment_lifecycle(current_status_sk);

-- Indexes cho Fact_Daily_Hub_Snapshot
CREATE INDEX IF NOT EXISTS idx_fdhs_date ON gold.fact_daily_hub_snapshot(snapshot_date_key);
CREATE INDEX IF NOT EXISTS idx_fdhs_hub_date ON gold.fact_daily_hub_snapshot(hub_sk, snapshot_date_key);