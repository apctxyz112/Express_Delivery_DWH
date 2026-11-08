-- Tầng Gold: 3 Bảng Fact chuẩn Kimball

-- Fact 1: Accumulating Snapshot Fact - Theo dõi toàn trình vận đơn
CREATE TABLE IF NOT EXISTS gold.fact_shipment_lifecycle (
    shipment_sk BIGSERIAL PRIMARY KEY,
    shipment_number VARCHAR(40) NOT NULL UNIQUE,
    sender_customer_sk INT NOT NULL REFERENCES gold.dim_customer(customer_sk),
    receiver_customer_sk INT NOT NULL REFERENCES gold.dim_customer(customer_sk),
    origin_hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),
    destination_hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),
    service_type_sk INT NOT NULL REFERENCES gold.dim_service_type(service_type_sk),
    payment_method_sk INT NOT NULL REFERENCES gold.dim_payment_method(payment_method_sk),
    current_status_sk INT NOT NULL REFERENCES gold.dim_status_reason(status_reason_sk),

    order_date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    pickup_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    first_sorting_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    last_sorting_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    first_delivery_attempt_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    final_delivery_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    return_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),
    promised_delivery_date_key INT NOT NULL DEFAULT -1 REFERENCES gold.dim_date(date_key),

    weight NUMERIC(10, 2) NOT NULL,
    freight_charge NUMERIC(12, 2) NOT NULL,
    cod_amount NUMERIC(12, 2) NOT NULL,
    distance_km NUMERIC(10, 2) NOT NULL,

    pickup_to_sorting_hours NUMERIC(10, 2),
    sorting_to_delivery_hours NUMERIC(10, 2),
    total_lead_time_hours NUMERIC(10, 2),

    is_delivered BOOLEAN NOT NULL DEFAULT FALSE,
    is_returned BOOLEAN NOT NULL DEFAULT FALSE,
    is_cancelled BOOLEAN NOT NULL DEFAULT FALSE,
    is_on_time BOOLEAN NOT NULL DEFAULT FALSE,
    is_sla_breached BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(64) NOT NULL
);

-- Fact 2: Transaction Fact - Sự kiện quét mã vạch trên hành trình
CREATE TABLE IF NOT EXISTS gold.fact_tracking_events (
    tracking_event_sk BIGSERIAL PRIMARY KEY,
    event_id_nk BIGINT NOT NULL UNIQUE,
    shipment_sk BIGINT NOT NULL REFERENCES gold.fact_shipment_lifecycle(shipment_sk),
    event_date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    event_timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),
    courier_sk INT NOT NULL REFERENCES gold.dim_courier(courier_sk),
    vehicle_sk INT NOT NULL REFERENCES gold.dim_vehicle(vehicle_sk),
    status_reason_sk INT NOT NULL REFERENCES gold.dim_status_reason(status_reason_sk),

    event_sequence INT NOT NULL DEFAULT 1,
    scan_source VARCHAR(30),
    batch_id VARCHAR(64) NOT NULL,
    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Fact 3: Periodic Snapshot Fact - Chốt tồn đọng và lưu lượng Hub theo ngày
CREATE TABLE IF NOT EXISTS gold.fact_daily_hub_snapshot (
    snapshot_date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),

    inbound_count INT NOT NULL DEFAULT 0,
    outbound_count INT NOT NULL DEFAULT 0,
    backlog_count INT NOT NULL DEFAULT 0,
    pending_delivery_count INT NOT NULL DEFAULT 0,
    return_count INT NOT NULL DEFAULT 0,
    cod_held_amount NUMERIC(14, 2) NOT NULL DEFAULT 0,

    batch_id VARCHAR(64) NOT NULL,
    snapshot_generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_date_key, hub_sk)
);