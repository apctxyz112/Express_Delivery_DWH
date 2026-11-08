-- Tầng Silver / Cleansed & Conformed: Đã qua Data Quality Gate, chuẩn hóa chuỗi và tính sẵn row_hash
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_id INT PRIMARY KEY,
    customer_code VARCHAR(30) NOT NULL,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,
    membership_tier VARCHAR(30) NOT NULL,
    phone_number VARCHAR(30),
    city VARCHAR(100),
    district VARCHAR(100),
    row_hash VARCHAR(64) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.hubs (
    hub_id INT PRIMARY KEY,
    hub_code VARCHAR(30) NOT NULL,
    hub_name VARCHAR(150) NOT NULL,
    hub_level VARCHAR(50) NOT NULL,
    city VARCHAR(100),
    district VARCHAR(100),
    capacity INT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.vehicles (
    vehicle_id INT PRIMARY KEY,
    vehicle_code VARCHAR(30) NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg NUMERIC(10, 2) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.couriers (
    courier_id INT PRIMARY KEY,
    courier_code VARCHAR(30) NOT NULL,
    courier_name VARCHAR(150) NOT NULL,
    hub_id INT,
    contract_type VARCHAR(30) NOT NULL,
    vehicle_id INT,
    row_hash VARCHAR(64) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.service_types (
    service_type_id INT PRIMARY KEY,
    service_code VARCHAR(30) NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    sla_hours INT NOT NULL,
    is_express BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.payment_methods (
    payment_method_id INT PRIMARY KEY,
    payment_method_code VARCHAR(30) NOT NULL,
    payment_method_name VARCHAR(100) NOT NULL,
    payer_type VARCHAR(50) NOT NULL,
    has_cod BOOLEAN NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.status_reasons (
    status_reason_id INT PRIMARY KEY,
    status_code VARCHAR(30) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    reason_code VARCHAR(30) NOT NULL,
    reason_name VARCHAR(150) NOT NULL,
    event_category VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.shipments (
    shipment_id BIGINT PRIMARY KEY,
    shipment_number VARCHAR(40) NOT NULL UNIQUE,
    sender_customer_id INT NOT NULL,
    receiver_customer_id INT NOT NULL,
    origin_hub_id INT NOT NULL,
    destination_hub_id INT NOT NULL,
    service_type_id INT NOT NULL,
    payment_method_id INT NOT NULL,
    weight NUMERIC(10, 2) NOT NULL,
    distance_km NUMERIC(10, 2) NOT NULL,
    freight_charge NUMERIC(12, 2) NOT NULL,
    cod_amount NUMERIC(12, 2) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    pickup_date TIMESTAMP,
    promised_delivery_date TIMESTAMP NOT NULL,
    actual_delivery_date TIMESTAMP,
    current_status VARCHAR(30) NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.shipment_tracking_events (
    event_id BIGINT PRIMARY KEY,
    shipment_id BIGINT NOT NULL,
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    hub_id INT,        -- Đã xử lý NULL hoặc giữ NULL để map sang SK = -1
    courier_id INT,    -- Đã xử lý NULL hoặc giữ NULL để map sang SK = -1
    vehicle_id INT,
    status_reason_id INT,
    scan_source VARCHAR(30),
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.cod_settlements (
    settlement_id BIGINT PRIMARY KEY,
    shipment_id BIGINT NOT NULL,
    cod_amount NUMERIC(12, 2) NOT NULL,
    settlement_status VARCHAR(30) NOT NULL,
    settled_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL,
    batch_id VARCHAR(64) NOT NULL
);