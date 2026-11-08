-- Tầng Bronze / Staging: Append dữ liệu kèm batch_id để truy vết chuẩn enterprise
CREATE TABLE IF NOT EXISTS stg.customers (
    customer_id INT,
    customer_code VARCHAR(30),
    customer_name VARCHAR(150),
    customer_type VARCHAR(30),
    membership_tier VARCHAR(30),
    phone_number VARCHAR(30),
    city VARCHAR(100),
    district VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.hubs (
    hub_id INT,
    hub_code VARCHAR(30),
    hub_name VARCHAR(150),
    hub_level VARCHAR(50),
    city VARCHAR(100),
    district VARCHAR(100),
    capacity INT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.vehicles (
    vehicle_id INT,
    vehicle_code VARCHAR(30),
    vehicle_type VARCHAR(50),
    capacity_kg NUMERIC(10, 2),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.couriers (
    courier_id INT,
    courier_code VARCHAR(30),
    courier_name VARCHAR(150),
    hub_id INT,
    contract_type VARCHAR(30),
    vehicle_id INT,
    effective_date DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.service_types (
    service_type_id INT,
    service_code VARCHAR(30),
    service_name VARCHAR(100),
    sla_hours INT,
    is_express BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.payment_methods (
    payment_method_id INT,
    payment_method_code VARCHAR(30),
    payment_method_name VARCHAR(100),
    payer_type VARCHAR(50),
    has_cod BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.status_reasons (
    status_reason_id INT,
    status_code VARCHAR(30),
    status_name VARCHAR(100),
    reason_code VARCHAR(30),
    reason_name VARCHAR(150),
    event_category VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.shipments (
    shipment_id BIGINT,
    shipment_number VARCHAR(40),
    sender_customer_id INT,
    receiver_customer_id INT,
    origin_hub_id INT,
    destination_hub_id INT,
    service_type_id INT,
    payment_method_id INT,
    weight NUMERIC(10, 2),
    distance_km NUMERIC(10, 2),
    freight_charge NUMERIC(12, 2),
    cod_amount NUMERIC(12, 2),
    order_date TIMESTAMP,
    pickup_date TIMESTAMP,
    promised_delivery_date TIMESTAMP,
    actual_delivery_date TIMESTAMP,
    current_status VARCHAR(30),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.shipment_tracking_events (
    event_id BIGINT,
    shipment_id BIGINT,
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    hub_id INT,
    courier_id INT,
    vehicle_id INT,
    status_reason_id INT,
    scan_source VARCHAR(30),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg.cod_settlements (
    settlement_id BIGINT,
    shipment_id BIGINT,
    cod_amount NUMERIC(12, 2),
    settlement_status VARCHAR(30),
    settled_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    batch_id VARCHAR(64),
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);