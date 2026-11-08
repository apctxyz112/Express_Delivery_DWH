-- Tạo toàn bộ các bảng OLTP cốt lõi cho hệ thống Chuyển phát nhanh
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    customer_code VARCHAR(30) NOT NULL UNIQUE,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,   -- Individual / Shop / B2B
    membership_tier VARCHAR(30) NOT NULL, -- Bronze / Silver / Gold
    phone_number VARCHAR(30),
    city VARCHAR(100),
    district VARCHAR(100),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hubs (
    hub_id SERIAL PRIMARY KEY,
    hub_code VARCHAR(30) NOT NULL UNIQUE,
    hub_name VARCHAR(150) NOT NULL,
    hub_level VARCHAR(50) NOT NULL,       -- Regional Hub / Local Branch
    city VARCHAR(100),
    district VARCHAR(100),
    capacity INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS vehicles (
    vehicle_id SERIAL PRIMARY KEY,
    vehicle_code VARCHAR(30) NOT NULL UNIQUE,
    vehicle_type VARCHAR(50) NOT NULL,    -- Motorbike / Van / Truck_2T / Truck_8T
    capacity_kg NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS couriers (
    courier_id SERIAL PRIMARY KEY,
    courier_code VARCHAR(30) NOT NULL UNIQUE,
    courier_name VARCHAR(150) NOT NULL,
    hub_id INT REFERENCES hubs(hub_id),
    contract_type VARCHAR(30) NOT NULL,   -- Full-time / Part-time
    vehicle_id INT REFERENCES vehicles(vehicle_id),
    effective_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS service_types (
    service_type_id SERIAL PRIMARY KEY,
    service_code VARCHAR(30) NOT NULL UNIQUE,
    service_name VARCHAR(100) NOT NULL,
    sla_hours INT NOT NULL,
    is_express BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS payment_methods (
    payment_method_id SERIAL PRIMARY KEY,
    payment_method_code VARCHAR(30) NOT NULL UNIQUE,
    payment_method_name VARCHAR(100) NOT NULL,
    payer_type VARCHAR(50) NOT NULL,      -- Sender / Receiver / Third-Party
    has_cod BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS status_reasons (
    status_reason_id SERIAL PRIMARY KEY,
    status_code VARCHAR(30) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    reason_code VARCHAR(30) NOT NULL UNIQUE,
    reason_name VARCHAR(150) NOT NULL,
    event_category VARCHAR(50) NOT NULL,  -- Normal / Exception / Terminal
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id BIGSERIAL PRIMARY KEY,
    shipment_number VARCHAR(40) NOT NULL UNIQUE,
    sender_customer_id INT REFERENCES customers(customer_id),
    receiver_customer_id INT REFERENCES customers(customer_id),
    origin_hub_id INT REFERENCES hubs(hub_id),
    destination_hub_id INT REFERENCES hubs(hub_id),
    service_type_id INT REFERENCES service_types(service_type_id),
    payment_method_id INT REFERENCES payment_methods(payment_method_id),
    weight NUMERIC(10, 2) NOT NULL,       -- Cho phép gài số <= 0 ở tầng OLTP để test Quarantine
    distance_km NUMERIC(10, 2) DEFAULT 15.0,
    freight_charge NUMERIC(12, 2) NOT NULL,
    cod_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
    order_date TIMESTAMP NOT NULL,
    pickup_date TIMESTAMP,
    promised_delivery_date TIMESTAMP NOT NULL,
    actual_delivery_date TIMESTAMP,
    current_status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipment_tracking_events (
    event_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES shipments(shipment_id),
    event_time TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,      -- ORDER_CREATED, PICKED_UP, INBOUND_SCAN, OUTBOUND_SCAN, DELIVERY_ATTEMPT, DELIVERED, RETURNED
    hub_id INT REFERENCES hubs(hub_id),   -- Cho phép NULL để test DQ Warning -> Map Unknown (-1)
    courier_id INT REFERENCES couriers(courier_id), -- Cho phép NULL để test DQ Warning -> Map Unknown (-1)
    vehicle_id INT REFERENCES vehicles(vehicle_id),
    status_reason_id INT REFERENCES status_reasons(status_reason_id),
    scan_source VARCHAR(30) DEFAULT 'PDA_SCANNER',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer_tickets (
    ticket_id SERIAL PRIMARY KEY,
    shipment_id BIGINT REFERENCES shipments(shipment_id),
    customer_id INT REFERENCES customers(customer_id),
    issue_type VARCHAR(50) NOT NULL,
    ticket_status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cod_settlements (
    settlement_id BIGSERIAL PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES shipments(shipment_id),
    cod_amount NUMERIC(12, 2) NOT NULL,
    settlement_status VARCHAR(30) NOT NULL, -- PENDING / SETTLED
    settled_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);