CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(30) UNIQUE NOT NULL,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,
    membership_tier VARCHAR(30) NOT NULL,
    phone_number VARCHAR(30),
    city VARCHAR(100),
    district VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS hubs (
    hub_id SERIAL PRIMARY KEY,
    hub_code VARCHAR(30) UNIQUE NOT NULL,
    hub_name VARCHAR(150) NOT NULL,
    hub_level VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    district VARCHAR(100) NOT NULL,
    capacity INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    vehicle_code VARCHAR(30) UNIQUE NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS couriers (
    courier_id SERIAL PRIMARY KEY,
    courier_code VARCHAR(30) UNIQUE NOT NULL,
    courier_name VARCHAR(150) NOT NULL,
    hub_id INT,
    contract_type VARCHAR(30) NOT NULL,
    vehicle_id INT,
    effective_date DATE,
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS service_types (
    service_type_id SERIAL PRIMARY KEY,
    service_code VARCHAR(30) UNIQUE NOT NULL,
    service_name VARCHAR(100) NOT NULL,
    sla_hours INT NOT NULL,
    is_express BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    payment_method_code VARCHAR(30) UNIQUE NOT NULL,
    payment_method_name VARCHAR(100) NOT NULL,
    payer_type VARCHAR(50) NOT NULL,
    has_cod BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS status_reasons (
    status_reason_id SERIAL PRIMARY KEY,
    status_code VARCHAR(30) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    reason_code VARCHAR(30) UNIQUE NOT NULL,
    reason_name VARCHAR(150) NOT NULL,
    event_category VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id BIGSERIAL PRIMARY KEY,
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
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipment_tracking_events (
    event_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT,
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    hub_id INT,
    courier_id INT,
    vehicle_id INT,
    status_reason_id INT,
    scan_source VARCHAR(30),
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS cod_settlements (
    settlement_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT,
    cod_amount NUMERIC(12, 2),
    settlement_status VARCHAR(30),
    settled_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_tickets (
    ticket_id SERIAL PRIMARY KEY,
    shipment_id BIGINT,
    customer_id INT,
    issue_type VARCHAR(50),
    ticket_status VARCHAR(30),
    created_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT LOCALTIMESTAMP
);