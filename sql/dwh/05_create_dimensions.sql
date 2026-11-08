-- Tầng Gold: 8 Dimensions theo thiết kế Kimball kèm dòng Unknown (SK = -1)

-- 1. Dim_Date
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    day_of_month INT NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_peak_season BOOLEAN NOT NULL
);

INSERT INTO gold.dim_date VALUES
(-1, '1900-01-01', 1900, 1, 1, 'Unknown', 1, 1, 1, FALSE, FALSE)
ON CONFLICT (date_key) DO NOTHING;

-- 2. Dim_Hub (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_hub (
    hub_sk SERIAL PRIMARY KEY,
    hub_nk VARCHAR(30) NOT NULL UNIQUE,
    hub_name VARCHAR(150) NOT NULL,
    hub_level VARCHAR(50) NOT NULL,
    city VARCHAR(100),
    district VARCHAR(100),
    capacity INT NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

INSERT INTO gold.dim_hub (hub_sk, hub_nk, hub_name, hub_level, city, district, capacity, is_current)
VALUES (-1, 'UNKNOWN', 'Unknown Hub', 'Unknown', 'Unknown', 'Unknown', 0, TRUE)
ON CONFLICT (hub_nk) DO NOTHING;

-- 3. Dim_Vehicle (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_vehicle (
    vehicle_sk SERIAL PRIMARY KEY,
    vehicle_nk VARCHAR(30) NOT NULL UNIQUE,
    vehicle_type VARCHAR(50) NOT NULL,
    capacity_kg NUMERIC(10, 2) NOT NULL
);

INSERT INTO gold.dim_vehicle (vehicle_sk, vehicle_nk, vehicle_type, capacity_kg)
VALUES (-1, 'UNKNOWN', 'Unknown Vehicle', 0)
ON CONFLICT (vehicle_nk) DO NOTHING;

-- 4. Dim_Service_Type (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_service_type (
    service_type_sk SERIAL PRIMARY KEY,
    service_type_nk VARCHAR(30) NOT NULL UNIQUE,
    service_name VARCHAR(100) NOT NULL,
    sla_hours INT NOT NULL,
    is_express BOOLEAN NOT NULL
);

INSERT INTO gold.dim_service_type (service_type_sk, service_type_nk, service_name, sla_hours, is_express)
VALUES (-1, 'UNKNOWN', 'Unknown Service', 0, FALSE)
ON CONFLICT (service_type_nk) DO NOTHING;

-- 5. Dim_Status_Reason (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_status_reason (
    status_reason_sk SERIAL PRIMARY KEY,
    status_code VARCHAR(30) NOT NULL,
    status_name VARCHAR(100) NOT NULL,
    reason_code VARCHAR(30) NOT NULL UNIQUE,
    reason_name VARCHAR(150) NOT NULL,
    event_category VARCHAR(50) NOT NULL
);

INSERT INTO gold.dim_status_reason (status_reason_sk, status_code, status_name, reason_code, reason_name, event_category)
VALUES (-1, 'UNKNOWN', 'Unknown Status', 'UNKNOWN', 'Unknown Reason', 'Unknown')
ON CONFLICT (reason_code) DO NOTHING;

-- 6. Dim_Payment_Method (SCD Type 1)
CREATE TABLE IF NOT EXISTS gold.dim_payment_method (
    payment_method_sk SERIAL PRIMARY KEY,
    payment_method_nk VARCHAR(30) NOT NULL UNIQUE,
    payment_method_name VARCHAR(100) NOT NULL,
    payer_type VARCHAR(50) NOT NULL,
    has_cod BOOLEAN NOT NULL
);

INSERT INTO gold.dim_payment_method (payment_method_sk, payment_method_nk, payment_method_name, payer_type, has_cod)
VALUES (-1, 'UNKNOWN', 'Unknown Payment Method', 'Unknown', FALSE)
ON CONFLICT (payment_method_nk) DO NOTHING;

-- 7. Dim_Customer (SCD Type 2)
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_nk VARCHAR(30) NOT NULL,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(30) NOT NULL,
    membership_tier VARCHAR(30) NOT NULL,
    city VARCHAR(100),
    district VARCHAR(100),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    row_hash VARCHAR(64) NOT NULL
);

INSERT INTO gold.dim_customer (customer_sk, customer_nk, customer_name, customer_type, membership_tier, city, district, valid_from, valid_to, is_current, row_hash)
SELECT -1, 'UNKNOWN', 'Unknown Customer', 'Unknown', 'Unknown', 'Unknown', 'Unknown', '1900-01-01 00:00:00', '9999-12-31 23:59:59', TRUE, 'UNKNOWN'
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer WHERE customer_sk = -1);

-- 8. Dim_Courier (SCD Type 2)
CREATE TABLE IF NOT EXISTS gold.dim_courier (
    courier_sk SERIAL PRIMARY KEY,
    courier_nk VARCHAR(30) NOT NULL,
    courier_name VARCHAR(150) NOT NULL,
    hub_sk INT NOT NULL REFERENCES gold.dim_hub(hub_sk),
    contract_type VARCHAR(30) NOT NULL,
    vehicle_sk INT NOT NULL REFERENCES gold.dim_vehicle(vehicle_sk),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    row_hash VARCHAR(64) NOT NULL
);

INSERT INTO gold.dim_courier (courier_sk, courier_nk, courier_name, hub_sk, contract_type, vehicle_sk, valid_from, valid_to, is_current, row_hash)
SELECT -1, 'UNKNOWN', 'Unknown Courier', -1, 'Unknown', -1, '1900-01-01 00:00:00', '9999-12-31 23:59:59', TRUE, 'UNKNOWN'
WHERE NOT EXISTS (SELECT 1 FROM gold.dim_courier WHERE courier_sk = -1);