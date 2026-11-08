Bản triển khai mã nguồn đầy đủ cho toàn bộ dự án **Express Delivery Data Warehouse** theo đúng **Master Implementation Blueprint (PostgreSQL + Python + Apache Airflow + Medallion Architecture + Kimball Dimensional Modeling)** với đầy đủ các tầng `stg`, `silver`, `gold`, `meta`, cơ chế **Data Quality Gate (Quarantine)**, **SCD Type 2 (Point-in-Time Join)** và **Idempotent Pipeline**.

---

## 1. Cấu trúc thư mục chuẩn của dự án

Toàn bộ các file dưới đây tuân thủ đúng cây thư mục mục 14 trong tài liệu thiết kế:

```text
express-delivery-dwh/
├── config/
│   ├── db_config.py
│   └── settings.py
├── sql/
│   ├── oltp/
│   │   ├── 01_create_oltp_tables.sql
│   │   └── 02_seed_constraints.sql
│   ├── dwh/
│   │   ├── 01_create_schemas.sql
│   │   ├── 02_create_meta_tables.sql
│   │   ├── 03_create_staging_tables.sql
│   │   ├── 04_create_silver_tables.sql
│   │   ├── 05_create_dimensions.sql
│   │   ├── 06_create_facts.sql
│   │   └── 07_create_indexes.sql
│   └── analytics/
│       ├── 01_create_views.sql
│       └── 02_create_materialized_views.sql
├── scripts/
│   ├── generate_oltp_data.py
│   ├── extract_incremental.py
│   ├── data_quality_checks.py
│   ├── load_dimensions.py
│   ├── load_facts.py
│   ├── refresh_views.py
│   └── run_local_pipeline.py
├── dags/
│   └── express_delivery_dwh_dag.py
└── docker/
    ├── docker-compose.yml
    └── requirements.txt
```

---

## 2. Nhóm Cấu hình (`config/`)

### File: `config/settings.py`

```python
import os

# Cấu hình kết nối PostgreSQL OLTP và DWH (Hỗ trợ cả chạy Local và trong Docker)
OLTP_DB_URI = os.getenv(
    "OLTP_DB_URI",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
)

DWH_DB_URI = os.getenv(
    "DWH_DB_URI",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/express_dwh"
)

# Quy mô sinh dữ liệu mặc định theo Blueprint
DEFAULT_CUSTOMERS = 1500
DEFAULT_HUBS = 30
DEFAULT_COURIERS = 200
DEFAULT_VEHICLES = 80
DEFAULT_SHIPMENTS = 10000
```

### File: `config/db_config.py`

```python
from sqlalchemy import create_engine
from contextlib import contextmanager
from config.settings import OLTP_DB_URI, DWH_DB_URI


oltp_engine = create_engine(OLTP_DB_URI, pool_pre_ping=True, future=True)
dwh_engine = create_engine(DWH_DB_URI, pool_pre_ping=True, future=True)

@contextmanager
def get_oltp_conn():
    conn = oltp_engine.connect()
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_dwh_conn():
    conn = dwh_engine.connect()
    try:
        yield conn
    finally:
        conn.close()
```

---

## 3. Nhóm SQL Nguồn OLTP (`sql/oltp/`)

### File: `sql/oltp/01_create_oltp_tables.sql`

```sql
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

```

### File: `sql/oltp/02_seed_constraints.sql`

```sql
-- Tạo Indexes trên cột updated_at của OLTP để tăng tốc Incremental Extract
CREATE INDEX IF NOT EXISTS idx_oltp_customers_updated ON customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_hubs_updated ON hubs(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_couriers_updated ON couriers(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_shipments_updated ON shipments(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_events_updated ON shipment_tracking_events(updated_at);
CREATE INDEX IF NOT EXISTS idx_oltp_events_shipment ON shipment_tracking_events(shipment_id);

-- Seed dữ liệu danh mục tĩnh chuẩn (Service Types, Payment Methods, Status Reasons)
INSERT INTO service_types (service_code, service_name, sla_hours, is_express) VALUES
('STD_ECON', 'Chuyển phát Tiết kiệm', 72, FALSE),
('STD_FAST', 'Chuyển phát Nhanh Liên tỉnh', 48, FALSE),
('EXP_24H', 'Chuyển phát Hỏa tốc 24H', 24, TRUE),
('EXP_SAME', 'Giao Ngay Nội Thành (Same-Day)', 12, TRUE),
('HEAVY_CARGO', 'Vận tải Hàng nặng B2B', 96, FALSE)
ON CONFLICT (service_code) DO NOTHING;

INSERT INTO payment_methods (payment_method_code, payment_method_name, payer_type, has_cod) VALUES
('CASH_SENDER', 'Tiền mặt Người gửi trả', 'Sender', FALSE),
('COD_RECEIVER', 'Thu hộ COD & Phí người nhận trả', 'Receiver', TRUE),
('COD_FREE_SHIP', 'Thu hộ COD - Shop trả cước', 'Sender', TRUE),
('WALLET_PREPAID', 'Ví điện tử trả trước', 'Sender', FALSE),
('B2B_MONTHLY', 'Công nợ cuối tháng B2B', 'Sender', FALSE)
ON CONFLICT (payment_method_code) DO NOTHING;

INSERT INTO status_reasons (status_code, status_name, reason_code, reason_name, event_category) VALUES
('CREATED', 'Mới tạo vận đơn', 'RS_INIT', 'Khách hàng tạo đơn trên hệ thống', 'Normal'),
('PICKED_UP', 'Đã lấy hàng', 'RS_PICK_OK', 'Bưu tá lấy hàng thành công tại điểm gửi', 'Normal'),
('IN_TRANSIT', 'Đang trung chuyển', 'RS_HUB_IN', 'Hàng nhập kho trung tâm phân loại (Inbound)', 'Normal'),
('IN_TRANSIT', 'Đang xuất kho', 'RS_HUB_OUT', 'Hàng xuất kho đi tuyến tiếp theo (Outbound)', 'Normal'),
('DELIVERING', 'Đang giao hàng', 'RS_OUT_DELIV', 'Shipper đang đi giao chặng cuối', 'Normal'),
('DELIVERED', 'Giao thành công', 'RS_DELIV_OK', 'Người nhận đã ký nhận hàng', 'Terminal'),
('DELIVERY_FAILED', 'Giao thất bại', 'RS_CUST_BUSY', 'Khách hẹn giao lại ca sau / Không nghe máy', 'Exception'),
('RETURNED', 'Hoàn hàng', 'RS_REFUSED', 'Người nhận từ chối nhận hàng', 'Terminal'),
('RETURNED', 'Hoàn hàng', 'RS_WRONG_ADDR', 'Sai địa chỉ / Số điện thoại không liên lạc được', 'Terminal'),
('CANCELLED', 'Đã hủy', 'RS_SHOP_CANCEL', 'Người gửi yêu cầu hủy đơn trước khi lấy', 'Terminal')
ON CONFLICT (reason_code) DO NOTHING;

```

---

## 4. Nhóm SQL Kho dữ liệu 3 Tầng (`sql/dwh/`)

### File: `sql/dwh/01_create_schemas.sql`

```sql
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS meta;

```

### File: `sql/dwh/02_create_meta_tables.sql`

```sql
CREATE TABLE IF NOT EXISTS meta.etl_watermark (
    table_name VARCHAR(100) PRIMARY KEY,
    last_extracted_timestamp TIMESTAMP NOT NULL DEFAULT '1970-01-01 00:00:00',
    last_batch_id VARCHAR(64),
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO meta.etl_watermark (table_name, last_extracted_timestamp) VALUES
('customers', '1970-01-01 00:00:00'),
('hubs', '1970-01-01 00:00:00'),
('vehicles', '1970-01-01 00:00:00'),
('couriers', '1970-01-01 00:00:00'),
('service_types', '1970-01-01 00:00:00'),
('payment_methods', '1970-01-01 00:00:00'),
('status_reasons', '1970-01-01 00:00:00'),
('shipments', '1970-01-01 00:00:00'),
('shipment_tracking_events', '1970-01-01 00:00:00'),
('cod_settlements', '1970-01-01 00:00:00')
ON CONFLICT (table_name) DO NOTHING;

CREATE TABLE IF NOT EXISTS meta.etl_batch_log (
    batch_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    run_start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_end TIMESTAMP,
    rows_extracted INT DEFAULT 0,
    rows_loaded INT DEFAULT 0,
    rows_rejected INT DEFAULT 0,
    status VARCHAR(30) NOT NULL, -- RUNNING / SUCCESS / FAILED
    error_message TEXT,
    PRIMARY KEY (batch_id, table_name)
);

CREATE TABLE IF NOT EXISTS meta.data_quarantine (
    quarantine_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(64) NOT NULL,
    source_table VARCHAR(100) NOT NULL,
    record_id VARCHAR(100) NOT NULL,
    rule_name VARCHAR(100) NOT NULL,
    severity VARCHAR(20) NOT NULL, -- Blocker / Warning
    error_message TEXT NOT NULL,
    payload JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_resolved BOOLEAN NOT NULL DEFAULT FALSE
);

```

### File: `sql/dwh/03_create_staging_tables.sql`

```sql
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

```

### File: `sql/dwh/04_create_silver_tables.sql`

```sql
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

```

### File: `sql/dwh/05_create_dimensions.sql`

```sql
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

```

### File: `sql/dwh/06_create_facts.sql`

```sql
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

```

### File: `sql/dwh/07_create_indexes.sql`

```sql
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

```

---

## 5. Nhóm SQL Semantic Layer & Analytics (`sql/analytics/`)

### File: `sql/analytics/01_create_views.sql`

```sql
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

```

### File: `sql/analytics/02_create_materialized_views.sql`

```sql
-- Materialized View 1: Hiệu năng & Tắc nghẽn Hub (Dwell Time + Backlog) cho Trang 2 Dashboard
DROP MATERIALIZED VIEW IF EXISTS gold.mv_daily_hub_performance;
CREATE MATERIALIZED VIEW gold.mv_daily_hub_performance AS
WITH hub_dwell AS (
    SELECT
        in_ev.hub_sk,
        in_ev.event_date_key AS snapshot_date_key,
        AVG(EXTRACT(EPOCH FROM (out_ev.event_timestamp - in_ev.event_timestamp)) / 3600.0) AS avg_dwell_time_hours
    FROM gold.fact_tracking_events in_ev
    JOIN gold.fact_tracking_events out_ev
      ON in_ev.shipment_sk = out_ev.shipment_sk
     AND in_ev.hub_sk = out_ev.hub_sk
     AND in_ev.event_type = 'INBOUND_SCAN'
     AND out_ev.event_type = 'OUTBOUND_SCAN'
     AND out_ev.event_timestamp >= in_ev.event_timestamp
    GROUP BY in_ev.hub_sk, in_ev.event_date_key
)
SELECT
    d.full_date AS snapshot_date,
    h.hub_nk AS hub_code,
    h.hub_name,
    h.hub_level,
    h.city,
    h.capacity,
    s.inbound_count,
    s.outbound_count,
    s.backlog_count,
    s.pending_delivery_count,
    s.return_count,
    s.cod_held_amount,
    ROUND(COALESCE(dw.avg_dwell_time_hours, 0)::numeric, 2) AS avg_dwell_time_hours,
    ROUND((100.0 * s.backlog_count / NULLIF(h.capacity, 0))::numeric, 2) AS backlog_capacity_ratio_pct
FROM gold.fact_daily_hub_snapshot s
JOIN gold.dim_date d ON s.snapshot_date_key = d.date_key
JOIN gold.dim_hub h ON s.hub_sk = h.hub_sk
LEFT JOIN hub_dwell dw ON s.hub_sk = dw.hub_sk AND s.snapshot_date_key = dw.snapshot_date_key;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hub_perf_unique
ON gold.mv_daily_hub_performance(snapshot_date, hub_code);

-- Materialized View 2: Hiệu suất Shipper & Phân tích Hoàn hàng (Trang 3 Dashboard - Chứng minh giá trị SCD2)
DROP MATERIALIZED VIEW IF EXISTS gold.mv_courier_performance;
CREATE MATERIALIZED VIEW gold.mv_courier_performance AS
SELECT
    d.year,
    d.month,
    c.courier_sk,
    c.courier_nk AS courier_code,
    c.courier_name,
    c.contract_type,
    c.is_current AS is_current_version,
    h.hub_name AS assigned_hub_at_event,
    COUNT(DISTINCT CASE WHEN e.event_type IN ('DELIVERED', 'RETURNED', 'DELIVERY_ATTEMPT') THEN e.shipment_sk END) AS total_handled_shipments,
    COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' THEN e.shipment_sk END) AS delivered_shipments,
    COUNT(DISTINCT CASE WHEN e.event_type = 'RETURNED' THEN e.shipment_sk END) AS returned_shipments,
    COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_sk END) AS on_time_delivered_shipments,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_sk END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' THEN e.shipment_sk END), 0),
        2
    ) AS courier_otd_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'RETURNED' THEN e.shipment_sk END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type IN ('DELIVERED', 'RETURNED') THEN e.shipment_sk END), 0),
        2
    ) AS courier_return_rate_pct
FROM gold.fact_tracking_events e
JOIN gold.fact_shipment_lifecycle f ON e.shipment_sk = f.shipment_sk
JOIN gold.dim_date d ON e.event_date_key = d.date_key
JOIN gold.dim_courier c ON e.courier_sk = c.courier_sk
JOIN gold.dim_hub h ON c.hub_sk = h.hub_sk
WHERE c.courier_sk <> -1
GROUP BY
    d.year, d.month,
    c.courier_sk, c.courier_nk, c.courier_name, c.contract_type, c.is_current,
    h.hub_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_courier_perf_unique
ON gold.mv_courier_performance(year, month, courier_sk);

```

---

## 6. Nhóm Scripts ETL & Sinh dữ liệu (`scripts/`)

### File: `scripts/generate_oltp_data.py`

```python
import random
from datetime import datetime, timedelta
from faker import Faker
from sqlalchemy import text
from config.db_config import get_oltp_conn
from config.settings import (
    DEFAULT_CUSTOMERS, DEFAULT_HUBS, DEFAULT_COURIERS,
    DEFAULT_VEHICLES, DEFAULT_SHIPMENTS
)

fake = Faker('vi_VN')
random.seed(42)

CITIES = [
    ("Hà Nội", ["Cầu Giấy", "Đống Đa", "Hoàng Mai", "Long Biên", "Nam Từ Liêm"]),
    ("TP. Hồ Chí Minh", ["Quận 1", "Quận 7", "Tân Bình", "Thủ Đức", "Bình Thạnh"]),
    ("Đà Nẵng", ["Hải Châu", "Thanh Khê", "Sơn Trà", "Ngũ Hành Sơn"]),
    ("Hải Phòng", ["Ngô Quyền", "Lê Chân", "Hồng Bàng"]),
    ("Cần Thơ", ["Ninh Kiều", "Bình Thủy", "Cái Răng"])
]

def generate_data(
    num_customers=DEFAULT_CUSTOMERS,
    num_hubs=DEFAULT_HUBS,
    num_vehicles=DEFAULT_VEHICLES,
    num_couriers=DEFAULT_COURIERS,
    num_shipments=DEFAULT_SHIPMENTS
):
    print("🚀 Bắt đầu khởi tạo dữ liệu giả lập OLTP (kèm dữ liệu bẩn có kiểm soát & kịch bản SCD2)...")
    base_time = datetime.now() - timedelta(days=30)

    with get_oltp_conn() as conn:
        # Kiểm tra nếu đã có dữ liệu thì chỉ sinh thêm batch mới hoặc mô phỏng thay đổi SCD2
        existing_shipments = conn.execute(text("SELECT COUNT(*) FROM shipments")).scalar()
        if existing_shipments > 0:
            print(f"ℹ️ OLTP đã có {existing_shipments} vận đơn. Tiến hành mô phỏng cập nhật SCD2 & đơn mới...")
            simulate_scd2_and_incremental_updates(conn)
            conn.commit()
            return

        # 1. Sinh Hubs
        hub_rows = []
        for i in range(1, num_hubs + 1):
            city, districts = random.choice(CITIES)
            district = random.choice(districts)
            hub_level = "Regional Hub" if i <= 6 else "Local Branch"
            hub_rows.append({
                "hub_code": f"HUB_{i:03d}",
                "hub_name": f"Bưu cục {district} - {city} #{i}",
                "hub_level": hub_level,
                "city": city,
                "district": district,
                "capacity": random.randint(2000, 8000) if hub_level == "Regional Hub" else random.randint(500, 2000),
                "created_at": base_time,
                "updated_at": base_time
            })
        conn.execute(text("""
            INSERT INTO hubs (hub_code, hub_name, hub_level, city, district, capacity, created_at, updated_at)
            VALUES (:hub_code, :hub_name, :hub_level, :city, :district, :capacity, :created_at, :updated_at)
        """), hub_rows)

        # 2. Sinh Vehicles
        vehicle_rows = []
        v_types = [("Motorbike", 50), ("Van", 800), ("Truck_2T", 2000), ("Truck_8T", 8000)]
        for i in range(1, num_vehicles + 1):
            v_type, cap = random.choice(v_types)
            vehicle_rows.append({
                "vehicle_code": f"VEH_{i:03d}",
                "vehicle_type": v_type,
                "capacity_kg": cap,
                "created_at": base_time,
                "updated_at": base_time
            })
        conn.execute(text("""
            INSERT INTO vehicles (vehicle_code, vehicle_type, capacity_kg, created_at, updated_at)
            VALUES (:vehicle_code, :vehicle_type, :capacity_kg, :created_at, :updated_at)
        """), vehicle_rows)

        hub_ids = [r[0] for r in conn.execute(text("SELECT hub_id FROM hubs")).fetchall()]
        vehicle_ids = [r[0] for r in conn.execute(text("SELECT vehicle_id FROM vehicles")).fetchall()]

        # 3. Sinh Customers (Cài cắm 2% số điện thoại sai định dạng)
        customer_rows = []
        for i in range(1, num_customers + 1):
            city, districts = random.choice(CITIES)
            is_bad_phone = (random.random() < 0.02)
            phone = "09" + "".join([str(random.randint(0, 9)) for _ in range(8)])
            if is_bad_phone:
                phone = "BAD_PHONE_###_" + str(i)

            customer_rows.append({
                "customer_code": f"CUST_{i:05d}",
                "customer_name": fake.name(),
                "customer_type": random.choice(["Individual", "Shop", "B2B"]),
                "membership_tier": random.choice(["Bronze", "Silver", "Gold"]),
                "phone_number": phone,
                "city": city,
                "district": random.choice(districts),
                "created_at": base_time,
                "updated_at": base_time
            })
        conn.execute(text("""
            INSERT INTO customers (customer_code, customer_name, customer_type, membership_tier, phone_number, city, district, created_at, updated_at)
            VALUES (:customer_code, :customer_name, :customer_type, :membership_tier, :phone_number, :city, :district, :created_at, :updated_at)
        """), customer_rows)

        # 4. Sinh Couriers
        courier_rows = []
        for i in range(1, num_couriers + 1):
            courier_rows.append({
                "courier_code": f"COUR_{i:04d}",
                "courier_name": fake.name(),
                "hub_id": random.choice(hub_ids),
                "contract_type": random.choice(["Full-time", "Part-time"]),
                "vehicle_id": random.choice(vehicle_ids),
                "effective_date": base_time.date(),
                "created_at": base_time,
                "updated_at": base_time
            })
        conn.execute(text("""
            INSERT INTO couriers (courier_code, courier_name, hub_id, contract_type, vehicle_id, effective_date, created_at, updated_at)
            VALUES (:courier_code, :courier_name, :hub_id, :contract_type, :vehicle_id, :effective_date, :created_at, :updated_at)
        """), courier_rows)

        cust_ids = [r[0] for r in conn.execute(text("SELECT customer_id FROM customers")).fetchall()]
        courier_ids = [r[0] for r in conn.execute(text("SELECT courier_id FROM couriers")).fetchall()]
        services = conn.execute(text("SELECT service_type_id, sla_hours FROM service_types")).fetchall()
        payments = conn.execute(text("SELECT payment_method_id, has_cod FROM payment_methods")).fetchall()
        reasons = {
            r[0]: r[1] for r in conn.execute(text("SELECT reason_code, status_reason_id FROM status_reasons")).fetchall()
        }

        # 5. Sinh 10,000 Shipments & ~45,000 Tracking Events
        shipment_rows = []
        for i in range(1, num_shipments + 1):
            sender_id, receiver_id = random.sample(cust_ids, 2)
            orig_hub, dest_hub = random.sample(hub_ids, 2)
            srv_id, sla_hours = random.choice(services)
            pay_id, has_cod = random.choice(payments)

            order_dt = base_time + timedelta(
                days=random.randint(0, 25),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            )
            pickup_dt = order_dt + timedelta(hours=random.randint(1, 6))
            promised_dt = order_dt + timedelta(hours=sla_hours)

            # Phân bổ trạng thái đơn
            rand_status = random.random()
            if rand_status < 0.78:
                current_status = "DELIVERED"
                # 85% giao đúng hạn, 15% trễ SLA
                if random.random() < 0.85:
                    actual_deliv_dt = order_dt + timedelta(hours=random.uniform(4, sla_hours * 0.95))
                else:
                    actual_deliv_dt = promised_dt + timedelta(hours=random.uniform(2, 36))
                updated_dt = actual_deliv_dt
            elif rand_status < 0.86:
                current_status = "RETURNED"
                actual_deliv_dt = order_dt + timedelta(hours=random.uniform(24, 72))
                updated_dt = actual_deliv_dt
            elif rand_status < 0.96:
                current_status = "IN_TRANSIT"
                actual_deliv_dt = None
                updated_dt = pickup_dt + timedelta(hours=random.uniform(2, 18))
            else:
                current_status = "CANCELLED"
                pickup_dt = None
                actual_deliv_dt = None
                updated_dt = order_dt + timedelta(hours=1)

            weight = round(random.uniform(0.2, 25.0), 2)

            # CÀI CẮM DỮ LIỆU BẨN (1% weight <= 0, 1% delivery_date < order_date)
            dirty_roll = random.random()
            if dirty_roll < 0.01:
                weight = round(random.uniform(-10.0, 0.0), 2)  # Blocker rule: weight <= 0
            elif dirty_roll < 0.02 and actual_deliv_dt is not None:
                actual_deliv_dt = order_dt - timedelta(hours=5) # Blocker rule: delivery_date < order_date

            freight_charge = round(20000 + weight * 4500, 2) if weight > 0 else 25000
            cod_amount = round(random.randint(10, 200) * 10000, 2) if has_cod else 0.0

            shipment_rows.append({
                "shipment_number": f"EXP{2026000000 + i}",
                "sender_customer_id": sender_id,
                "receiver_customer_id": receiver_id,
                "origin_hub_id": orig_hub,
                "destination_hub_id": dest_hub,
                "service_type_id": srv_id,
                "payment_method_id": pay_id,
                "weight": weight,
                "distance_km": round(random.uniform(5.0, 1200.0), 1),
                "freight_charge": freight_charge,
                "cod_amount": cod_amount,
                "order_date": order_dt,
                "pickup_date": pickup_dt,
                "promised_delivery_date": promised_dt,
                "actual_delivery_date": actual_deliv_dt,
                "current_status": current_status,
                "created_at": order_dt,
                "updated_at": updated_dt
            })

        conn.execute(text("""
            INSERT INTO shipments (
                shipment_number, sender_customer_id, receiver_customer_id,
                origin_hub_id, destination_hub_id, service_type_id, payment_method_id,
                weight, distance_km, freight_charge, cod_amount,
                order_date, pickup_date, promised_delivery_date, actual_delivery_date,
                current_status, created_at, updated_at
            ) VALUES (
                :shipment_number, :sender_customer_id, :receiver_customer_id,
                :origin_hub_id, :destination_hub_id, :service_type_id, :payment_method_id,
                :weight, :distance_km, :freight_charge, :cod_amount,
                :order_date, :pickup_date, :promised_delivery_date, :actual_delivery_date,
                :current_status, :created_at, :updated_at
            )
        """), shipment_rows)

        # Lấy danh sách shipments vừa tạo để sinh Tracking Events và COD Settlements
        inserted_shipments = conn.execute(text("""
            SELECT shipment_id, origin_hub_id, destination_hub_id,
                   order_date, pickup_date, actual_delivery_date, current_status, cod_amount
            FROM shipments
        """)).fetchall()

        event_rows = []
        cod_rows = []

        for sh in inserted_shipments:
            sh_id, orig_h, dest_h, ord_dt, pick_dt, deliv_dt, st_code, cod_amt = sh
            c1, c2 = random.sample(courier_ids, 2)
            v1, v2 = random.sample(vehicle_ids, 2)

            # Event 1: ORDER_CREATED
            event_rows.append({
                "shipment_id": sh_id,
                "event_time": ord_dt,
                "event_type": "ORDER_CREATED",
                "hub_id": orig_h,
                "courier_id": c1,
                "vehicle_id": v1,
                "status_reason_id": reasons["RS_INIT"],
                "scan_source": "CUSTOMER_APP",
                "created_at": ord_dt,
                "updated_at": ord_dt
            })

            if st_code == "CANCELLED" or pick_dt is None:
                continue

            # Event 2: PICKED_UP (Cài cắm 2% thiếu courier_id, 1% thiếu hub_id)
            ev_hub = None if random.random() < 0.01 else orig_h
            ev_courier = None if random.random() < 0.02 else c1
            event_rows.append({
                "shipment_id": sh_id,
                "event_time": pick_dt,
                "event_type": "PICKED_UP",
                "hub_id": ev_hub,
                "courier_id": ev_courier,
                "vehicle_id": v1,
                "status_reason_id": reasons["RS_PICK_OK"],
                "scan_source": "PDA_SCANNER",
                "created_at": pick_dt,
                "updated_at": pick_dt
            })

            # Event 3: INBOUND_SCAN tại Origin Hub
            in_orig_dt = pick_dt + timedelta(hours=random.uniform(1.0, 3.0))
            event_rows.append({
                "shipment_id": sh_id,
                "event_time": in_orig_dt,
                "event_type": "INBOUND_SCAN",
                "hub_id": orig_h,
                "courier_id": c1,
                "vehicle_id": v1,
                "status_reason_id": reasons["RS_HUB_IN"],
                "scan_source": "HUB_GATE_SCAN",
                "created_at": in_orig_dt,
                "updated_at": in_orig_dt
            })

            # Event 4: OUTBOUND_SCAN tại Origin Hub
            out_orig_dt = in_orig_dt + timedelta(hours=random.uniform(1.5, 6.0))
            event_rows.append({
                "shipment_id": sh_id,
                "event_time": out_orig_dt,
                "event_type": "OUTBOUND_SCAN",
                "hub_id": orig_h,
                "courier_id": c1,
                "vehicle_id": v2,
                "status_reason_id": reasons["RS_HUB_OUT"],
                "scan_source": "HUB_GATE_SCAN",
                "created_at": out_orig_dt,
                "updated_at": out_orig_dt
            })

            if st_code in ("DELIVERED", "RETURNED") and deliv_dt is not None and deliv_dt > out_orig_dt:
                # Event 5: INBOUND_SCAN tại Destination Hub
                in_dest_dt = out_orig_dt + (deliv_dt - out_orig_dt) * 0.6
                event_rows.append({
                    "shipment_id": sh_id,
                    "event_time": in_dest_dt,
                    "event_type": "INBOUND_SCAN",
                    "hub_id": dest_h,
                    "courier_id": c2,
                    "vehicle_id": v2,
                    "status_reason_id": reasons["RS_HUB_IN"],
                    "scan_source": "HUB_GATE_SCAN",
                    "created_at": in_dest_dt,
                    "updated_at": in_dest_dt
                })

                # Event 6: Final DELIVERED hoặc RETURNED
                final_reason = reasons["RS_DELIV_OK"] if st_code == "DELIVERED" else reasons["RS_REFUSED"]
                event_rows.append({
                    "shipment_id": sh_id,
                    "event_time": deliv_dt,
                    "event_type": st_code,
                    "hub_id": dest_h,
                    "courier_id": c2,
                    "vehicle_id": v2,
                    "status_reason_id": final_reason,
                    "scan_source": "PDA_SCANNER",
                    "created_at": deliv_dt,
                    "updated_at": deliv_dt
                })

            if cod_amt > 0:
                is_settled = (st_code == "DELIVERED" and random.random() < 0.7)
                cod_rows.append({
                    "shipment_id": sh_id,
                    "cod_amount": cod_amt,
                    "settlement_status": "SETTLED" if is_settled else "PENDING",
                    "settled_at": (deliv_dt + timedelta(days=1)) if (is_settled and deliv_dt) else None,
                    "created_at": ord_dt,
                    "updated_at": ord_dt
                })

        # Chia batch insert cho 45,000+ events
        batch_size = 5000
        for idx in range(0, len(event_rows), batch_size):
            conn.execute(text("""
                INSERT INTO shipment_tracking_events (
                    shipment_id, event_time, event_type, hub_id, courier_id,
                    vehicle_id, status_reason_id, scan_source, created_at, updated_at
                ) VALUES (
                    :shipment_id, :event_time, :event_type, :hub_id, :courier_id,
                    :vehicle_id, :status_reason_id, :scan_source, :created_at, :updated_at
                )
            """), event_rows[idx:idx + batch_size])

        if cod_rows:
            conn.execute(text("""
                INSERT INTO cod_settlements (shipment_id, cod_amount, settlement_status, settled_at, created_at, updated_at)
                VALUES (:shipment_id, :cod_amount, :settlement_status, :settled_at, :created_at, :updated_at)
            """), cod_rows)

        conn.commit()
        print(f"✅ Đã sinh thành công {len(shipment_rows)} Shipments và {len(event_rows)} Tracking Events!")

def simulate_scd2_and_incremental_updates(conn):
    """
    Hàm mô phỏng thay đổi dữ liệu ở lần chạy thứ 2 để chứng minh:
    1. Khách hàng thăng hạng Membership Tier (SCD Type 2)
    2. Shipper điều chuyển sang Hub mới (SCD Type 2)
    3. Các vận đơn đang IN_TRANSIT chuyển sang DELIVERED (Accumulating Fact MERGE/UPSERT)
    """
    now_ts = datetime.now()
    conn.execute(text("""
        UPDATE customers
        SET membership_tier = CASE WHEN membership_tier = 'Bronze' THEN 'Silver' ELSE 'Gold' END,
            updated_at = :now_ts
        WHERE customer_id IN (SELECT customer_id FROM customers ORDER BY customer_id LIMIT 25)
    """), {"now_ts": now_ts})

    conn.execute(text("""
        UPDATE couriers
        SET hub_id = (SELECT MAX(hub_id) FROM hubs),
            contract_type = 'Full-time',
            updated_at = :now_ts
        WHERE courier_id IN (SELECT courier_id FROM couriers ORDER BY courier_id LIMIT 15)
    """), {"now_ts": now_ts})

    conn.execute(text("""
        UPDATE shipments
        SET current_status = 'DELIVERED',
            actual_delivery_date = :now_ts,
            updated_at = :now_ts
        WHERE shipment_id IN (
            SELECT shipment_id FROM shipments WHERE current_status = 'IN_TRANSIT' LIMIT 50
        )
    """), {"now_ts": now_ts})
    print("✅ Đã cập nhật 25 Customers (đổi hạng), 15 Couriers (chuyển Hub) và 50 Shipments (giao hoàn tất)!")

if __name__ == "__main__":
    generate_data()
```

### File: `scripts/extract_incremental.py`

```python
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from config.db_config import get_oltp_conn, get_dwh_conn

TABLES_TO_EXTRACT = [
    "customers", "hubs", "vehicles", "couriers", "service_types",
    "payment_methods", "status_reasons", "shipments",
    "shipment_tracking_events", "cod_settlements"
]

def run_extract_incremental(batch_id: str):
    print(f"📥 [Batch: {batch_id}] Bắt đầu Incremental Extract từ OLTP sang Staging...")
    extracted_counts = {}

    with get_dwh_conn() as dwh_conn, get_oltp_conn() as oltp_conn:
        for table in TABLES_TO_EXTRACT:
            run_start = datetime.now()
            dwh_conn.execute(text("""
                INSERT INTO meta.etl_batch_log (batch_id, table_name, run_start, status)
                VALUES (:batch_id, :table_name, :run_start, 'RUNNING')
                ON CONFLICT (batch_id, table_name) DO UPDATE
                SET run_start = EXCLUDED.run_start, status = 'RUNNING', error_message = NULL
            """), {"batch_id": batch_id, "table_name": table, "run_start": run_start})
            dwh_conn.commit()

            try:
                wm_ts = dwh_conn.execute(text("""
                    SELECT last_extracted_timestamp FROM meta.etl_watermark WHERE table_name = :table_name
                """), {"table_name": table}).scalar() or datetime(1970, 1, 1)

                # CHUẨN ENTERPRISE: Dùng text() và bindparams()
                query = text(f"SELECT * FROM {table} WHERE updated_at > :wm_ts").bindparams(wm_ts=wm_ts)
            
                # Pandas 2.1.4 + SQLAlchemy 1.4 (future=True) = Hoạt động hoàn hảo
                df = pd.read_sql(query, oltp_conn)
                row_count = len(df)
                extracted_counts[table] = row_count

                dwh_conn.execute(text(f"DELETE FROM stg.{table} WHERE batch_id = :batch_id"), {"batch_id": batch_id})

                if row_count > 0:
                    df["batch_id"] = batch_id
                    df["extracted_at"] = datetime.now()
                    df.to_sql(
                        name=table, con=dwh_conn, schema="stg",
                        if_exists="append", index=False, method="multi", chunksize=2000
                    )

                dwh_conn.execute(text("""
                    UPDATE meta.etl_batch_log
                    SET rows_extracted = :rows_extracted, status = 'EXTRACTED'
                    WHERE batch_id = :batch_id AND table_name = :table_name
                """), {"rows_extracted": row_count, "batch_id": batch_id, "table_name": table})
                dwh_conn.commit()
                print(f"   ✔ {table}: Extracted {row_count} rows (Watermark > {wm_ts})")

            except Exception as exc:
                dwh_conn.execute(text("""
                    UPDATE meta.etl_batch_log
                    SET run_end = CURRENT_TIMESTAMP, status = 'FAILED', error_message = :err
                    WHERE batch_id = :batch_id AND table_name = :table_name
                """), {"err": str(exc), "batch_id": batch_id, "table_name": table})
                dwh_conn.commit()
                raise

    return extracted_counts
```

### File: `scripts/data_quality_checks.py`

```python
from sqlalchemy import text
from config.db_config import get_dwh_conn

def run_data_quality_and_silver_load(batch_id: str):
    print(f"🛡️ [Batch: {batch_id}] Chạy Data Quality Gate & Chuẩn hóa dữ liệu vào tầng Silver...")

    with get_dwh_conn() as conn:
        # Xóa bản ghi quarantine cũ nếu chạy lại cùng batch_id (Idempotent)
        conn.execute(text("DELETE FROM meta.data_quarantine WHERE batch_id = :batch_id"), {"batch_id": batch_id})

        # =====================================================================
        # 1. KIỂM TRA RULES TRÊN BẢNG SHIPMENTS (BLOCKER RULES)
        # =====================================================================
        # Rule 1: weight <= 0 (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', shipment_number,
                'RULE_WEIGHT_POSITIVE', 'Blocker',
                'Trọng lượng vận đơn nhỏ hơn hoặc bằng 0 kg',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id AND (s.weight IS NULL OR s.weight <= 0);
        """), {"batch_id": batch_id})

        # Rule 2: actual_delivery_date < order_date (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', shipment_number,
                'RULE_DELIVERY_AFTER_ORDER', 'Blocker',
                'Thời gian giao hàng thực tế nhỏ hơn thời gian đặt hàng',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND s.actual_delivery_date IS NOT NULL
              AND s.actual_delivery_date < s.order_date;
        """), {"batch_id": batch_id})

        # Rule 3: Thiếu sender_customer_id hoặc trạng thái không hợp lệ (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipments', COALESCE(shipment_number, 'UNKNOWN'),
                'RULE_VALID_SHIPMENT_CORE', 'Blocker',
                'Thiếu khách gửi hoặc trạng thái vận đơn không nằm trong danh mục chuẩn',
                to_jsonb(s)
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND (
                  s.sender_customer_id IS NULL
                  OR s.current_status NOT IN ('CREATED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERING', 'DELIVERED', 'RETURNED', 'CANCELLED')
              );
        """), {"batch_id": batch_id})

        # =====================================================================
        # 2. KIỂM TRA RULES TRÊN BẢNG TRACKING_EVENTS & CUSTOMERS (BLOCKER + WARNING)
        # =====================================================================
        # Rule 4: event_time < order_date (Blocker)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipment_tracking_events', CAST(e.event_id AS VARCHAR),
                'RULE_EVENT_TIME_VALID', 'Blocker',
                'Thời gian quét sự kiện nhỏ hơn thời gian tạo vận đơn gốc',
                to_jsonb(e)
            FROM stg.shipment_tracking_events e
            JOIN stg.shipments s ON e.shipment_id = s.shipment_id AND s.batch_id = :batch_id
            WHERE e.batch_id = :batch_id AND e.event_time < s.order_date;
        """), {"batch_id": batch_id})

        # Rule 5: Thiếu courier_id hoặc hub_id trong tracking_events (Warning - Vẫn cho qua Silver & Map Unknown -1)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'shipment_tracking_events', CAST(e.event_id AS VARCHAR),
                'RULE_MISSING_HUB_OR_COURIER', 'Warning',
                'Sự kiện thiếu hub_id hoặc courier_id -> Sẽ map vào dòng Unknown (SK = -1)',
                to_jsonb(e)
            FROM stg.shipment_tracking_events e
            WHERE e.batch_id = :batch_id AND (e.hub_id IS NULL OR e.courier_id IS NULL);
        """), {"batch_id": batch_id})

        # Rule 6: Số điện thoại sai định dạng ở bảng customers (Warning - Chuẩn hóa lại số điện thoại)
        conn.execute(text("""
            INSERT INTO meta.data_quarantine (batch_id, source_table, record_id, rule_name, severity, error_message, payload)
            SELECT
                :batch_id, 'customers', customer_code,
                'RULE_INVALID_PHONE_FORMAT', 'Warning',
                'Số điện thoại chứa ký tự không hợp lệ -> Đã được chuẩn hóa tại tầng Silver',
                to_jsonb(c)
            FROM stg.customers c
            WHERE c.batch_id = :batch_id AND c.phone_number !~ '^[0-9]{9,12}$';
        """), {"batch_id": batch_id})

        # =====================================================================
        # 3. NẠP DỮ LIỆU HỢP LỆ TỪ STG SANG SILVER (UPSERT + TÍNH ROW_HASH CHO SCD2)
        # =====================================================================
        # 3.1. Silver Customers
        conn.execute(text("""
            INSERT INTO silver.customers (
                customer_id, customer_code, customer_name, customer_type,
                membership_tier, phone_number, city, district, row_hash, updated_at, batch_id
            )
            SELECT DISTINCT ON (customer_id)
                customer_id,
                TRIM(customer_code),
                TRIM(customer_name),
                TRIM(customer_type),
                TRIM(membership_tier),
                CASE
                    WHEN REGEXP_REPLACE(COALESCE(phone_number, ''), '[^0-9]', '', 'g') = '' THEN '0000000000'
                    ELSE SUBSTRING(REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g') FROM 1 FOR 15)
                END AS phone_number,
                TRIM(city),
                TRIM(district),
                MD5(
                    COALESCE(TRIM(customer_name), '') || '|' ||
                    COALESCE(TRIM(customer_type), '') || '|' ||
                    COALESCE(TRIM(membership_tier), '') || '|' ||
                    COALESCE(TRIM(city), '') || '|' ||
                    COALESCE(TRIM(district), '')
                ) AS row_hash,
                updated_at,
                :batch_id
            FROM stg.customers
            WHERE batch_id = :batch_id
            ORDER BY customer_id, updated_at DESC
            ON CONFLICT (customer_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                customer_type = EXCLUDED.customer_type,
                membership_tier = EXCLUDED.membership_tier,
                phone_number = EXCLUDED.phone_number,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                row_hash = EXCLUDED.row_hash,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.2. Silver Hubs
        conn.execute(text("""
            INSERT INTO silver.hubs (hub_id, hub_code, hub_name, hub_level, city, district, capacity, updated_at, batch_id)
            SELECT DISTINCT ON (hub_id)
                hub_id, TRIM(hub_code), TRIM(hub_name), TRIM(hub_level),
                TRIM(city), TRIM(district), capacity, updated_at, :batch_id
            FROM stg.hubs
            WHERE batch_id = :batch_id
            ORDER BY hub_id, updated_at DESC
            ON CONFLICT (hub_id) DO UPDATE SET
                hub_name = EXCLUDED.hub_name,
                hub_level = EXCLUDED.hub_level,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                capacity = EXCLUDED.capacity,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.3. Silver Vehicles
        conn.execute(text("""
            INSERT INTO silver.vehicles (vehicle_id, vehicle_code, vehicle_type, capacity_kg, updated_at, batch_id)
            SELECT DISTINCT ON (vehicle_id)
                vehicle_id, TRIM(vehicle_code), TRIM(vehicle_type), capacity_kg, updated_at, :batch_id
            FROM stg.vehicles
            WHERE batch_id = :batch_id
            ORDER BY vehicle_id, updated_at DESC
            ON CONFLICT (vehicle_id) DO UPDATE SET
                vehicle_type = EXCLUDED.vehicle_type,
                capacity_kg = EXCLUDED.capacity_kg,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.4. Silver Couriers
        conn.execute(text("""
            INSERT INTO silver.couriers (
                courier_id, courier_code, courier_name, hub_id,
                contract_type, vehicle_id, row_hash, updated_at, batch_id
            )
            SELECT DISTINCT ON (courier_id)
                courier_id,
                TRIM(courier_code),
                TRIM(courier_name),
                hub_id,
                TRIM(contract_type),
                vehicle_id,
                MD5(
                    COALESCE(TRIM(courier_name), '') || '|' ||
                    COALESCE(CAST(hub_id AS VARCHAR), '-1') || '|' ||
                    COALESCE(TRIM(contract_type), '') || '|' ||
                    COALESCE(CAST(vehicle_id AS VARCHAR), '-1')
                ) AS row_hash,
                updated_at,
                :batch_id
            FROM stg.couriers
            WHERE batch_id = :batch_id
            ORDER BY courier_id, updated_at DESC
            ON CONFLICT (courier_id) DO UPDATE SET
                courier_name = EXCLUDED.courier_name,
                hub_id = EXCLUDED.hub_id,
                contract_type = EXCLUDED.contract_type,
                vehicle_id = EXCLUDED.vehicle_id,
                row_hash = EXCLUDED.row_hash,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.5. Silver Service Types, Payment Methods, Status Reasons
        conn.execute(text("""
            INSERT INTO silver.service_types (service_type_id, service_code, service_name, sla_hours, is_express, updated_at, batch_id)
            SELECT DISTINCT ON (service_type_id)
                service_type_id, TRIM(service_code), TRIM(service_name), sla_hours, is_express, updated_at, :batch_id
            FROM stg.service_types WHERE batch_id = :batch_id
            ORDER BY service_type_id, updated_at DESC
            ON CONFLICT (service_type_id) DO UPDATE SET
                service_name = EXCLUDED.service_name, sla_hours = EXCLUDED.sla_hours,
                is_express = EXCLUDED.is_express, updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id;

            INSERT INTO silver.payment_methods (payment_method_id, payment_method_code, payment_method_name, payer_type, has_cod, updated_at, batch_id)
            SELECT DISTINCT ON (payment_method_id)
                payment_method_id, TRIM(payment_method_code), TRIM(payment_method_name), TRIM(payer_type), has_cod, updated_at, :batch_id
            FROM stg.payment_methods WHERE batch_id = :batch_id
            ORDER BY payment_method_id, updated_at DESC
            ON CONFLICT (payment_method_id) DO UPDATE SET
                payment_method_name = EXCLUDED.payment_method_name, payer_type = EXCLUDED.payer_type,
                has_cod = EXCLUDED.has_cod, updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id;

            INSERT INTO silver.status_reasons (status_reason_id, status_code, status_name, reason_code, reason_name, event_category, updated_at, batch_id)
            SELECT DISTINCT ON (status_reason_id)
                status_reason_id, TRIM(status_code), TRIM(status_name), TRIM(reason_code), TRIM(reason_name), TRIM(event_category), updated_at, :batch_id
            FROM stg.status_reasons WHERE batch_id = :batch_id
            ORDER BY status_reason_id, updated_at DESC
            ON CONFLICT (status_reason_id) DO UPDATE SET
                status_name = EXCLUDED.status_name, reason_name = EXCLUDED.reason_name,
                event_category = EXCLUDED.event_category, updated_at = EXCLUDED.updated_at, batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.6. Silver Shipments (Loại bỏ tất cả các bản ghi dính lỗi Blocker trong Quarantine)
        conn.execute(text("""
            INSERT INTO silver.shipments (
                shipment_id, shipment_number, sender_customer_id, receiver_customer_id,
                origin_hub_id, destination_hub_id, service_type_id, payment_method_id,
                weight, distance_km, freight_charge, cod_amount,
                order_date, pickup_date, promised_delivery_date, actual_delivery_date,
                current_status, updated_at, batch_id
            )
            SELECT DISTINCT ON (s.shipment_id)
                s.shipment_id, TRIM(s.shipment_number), s.sender_customer_id, s.receiver_customer_id,
                s.origin_hub_id, s.destination_hub_id, s.service_type_id, s.payment_method_id,
                s.weight, COALESCE(s.distance_km, 10.0), s.freight_charge, COALESCE(s.cod_amount, 0),
                s.order_date, s.pickup_date, s.promised_delivery_date, s.actual_delivery_date,
                TRIM(s.current_status), s.updated_at, :batch_id
            FROM stg.shipments s
            WHERE s.batch_id = :batch_id
              AND NOT EXISTS (
                  SELECT 1 FROM meta.data_quarantine q
                  WHERE q.batch_id = :batch_id
                    AND q.source_table = 'shipments'
                    AND q.severity = 'Blocker'
                    AND q.record_id = s.shipment_number
              )
            ORDER BY s.shipment_id, s.updated_at DESC
            ON CONFLICT (shipment_id) DO UPDATE SET
                pickup_date = EXCLUDED.pickup_date,
                actual_delivery_date = EXCLUDED.actual_delivery_date,
                current_status = EXCLUDED.current_status,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.7. Silver Tracking Events (Chỉ giữ events của các Shipments hợp lệ và không dính Blocker)
        conn.execute(text("""
            INSERT INTO silver.shipment_tracking_events (
                event_id, shipment_id, event_time, event_type,
                hub_id, courier_id, vehicle_id, status_reason_id,
                scan_source, updated_at, batch_id
            )
            SELECT DISTINCT ON (e.event_id)
                e.event_id, e.shipment_id, e.event_time, TRIM(e.event_type),
                e.hub_id, e.courier_id, e.vehicle_id, e.status_reason_id,
                TRIM(e.scan_source), e.updated_at, :batch_id
            FROM stg.shipment_tracking_events e
            JOIN silver.shipments ss ON e.shipment_id = ss.shipment_id
            WHERE e.batch_id = :batch_id
              AND NOT EXISTS (
                  SELECT 1 FROM meta.data_quarantine q
                  WHERE q.batch_id = :batch_id
                    AND q.source_table = 'shipment_tracking_events'
                    AND q.severity = 'Blocker'
                    AND q.record_id = CAST(e.event_id AS VARCHAR)
              )
            ORDER BY e.event_id, e.updated_at DESC
            ON CONFLICT (event_id) DO UPDATE SET
                event_time = EXCLUDED.event_time,
                event_type = EXCLUDED.event_type,
                hub_id = EXCLUDED.hub_id,
                courier_id = EXCLUDED.courier_id,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # 3.8. Silver COD Settlements
        conn.execute(text("""
            INSERT INTO silver.cod_settlements (
                settlement_id, shipment_id, cod_amount, settlement_status, settled_at, updated_at, batch_id
            )
            SELECT DISTINCT ON (c.settlement_id)
                c.settlement_id, c.shipment_id, c.cod_amount, TRIM(c.settlement_status), c.settled_at, c.updated_at, :batch_id
            FROM stg.cod_settlements c
            JOIN silver.shipments ss ON c.shipment_id = ss.shipment_id
            WHERE c.batch_id = :batch_id
            ORDER BY c.settlement_id, c.updated_at DESC
            ON CONFLICT (settlement_id) DO UPDATE SET
                settlement_status = EXCLUDED.settlement_status,
                settled_at = EXCLUDED.settled_at,
                updated_at = EXCLUDED.updated_at,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # Cập nhật số dòng bị từ chối (Blocker) vào meta.etl_batch_log
        conn.execute(text("""
            UPDATE meta.etl_batch_log b
            SET rows_rejected = (
                SELECT COUNT(*) FROM meta.data_quarantine q
                WHERE q.batch_id = b.batch_id AND q.source_table = b.table_name AND q.severity = 'Blocker'
            )
            WHERE b.batch_id = :batch_id;
        """), {"batch_id": batch_id})

        blocker_cnt = conn.execute(text("""
            SELECT COUNT(*) FROM meta.data_quarantine WHERE batch_id = :batch_id AND severity = 'Blocker'
        """), {"batch_id": batch_id}).scalar()
        warning_cnt = conn.execute(text("""
            SELECT COUNT(*) FROM meta.data_quarantine WHERE batch_id = :batch_id AND severity = 'Warning'
        """), {"batch_id": batch_id}).scalar()

        conn.commit()
        print(f"✅ Data Quality Gate hoàn tất: {blocker_cnt} bản ghi Blocker bị cách ly, {warning_cnt} cảnh báo Warning đã được xử lý.")
```

### File: `scripts/load_dimensions.py`

```python
from sqlalchemy import text
from config.db_config import get_dwh_conn

def run_load_dimensions(batch_id: str):
    print(f"🏛️ [Batch: {batch_id}] Đang nạp 8 bảng Dimensions (SCD Type 1 & SCD Type 2)...")

    with get_dwh_conn() as conn:
        # 1. Nạp Dim_Date (Tự động khởi tạo lịch từ 2024 đến 2028)
        conn.execute(text("""
            INSERT INTO gold.dim_date (
                date_key, full_date, year, quarter, month, month_name,
                week, day_of_week, day_of_month, is_weekend, is_peak_season
            )
            SELECT
                TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
                d::DATE AS full_date,
                EXTRACT(YEAR FROM d)::INT AS year,
                EXTRACT(QUARTER FROM d)::INT AS quarter,
                EXTRACT(MONTH FROM d)::INT AS month,
                TRIM(TO_CHAR(d, 'Month')) AS month_name,
                EXTRACT(WEEK FROM d)::INT AS week,
                EXTRACT(ISODOW FROM d)::INT AS day_of_week,
                EXTRACT(DAY FROM d)::INT AS day_of_month,
                CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
                CASE WHEN EXTRACT(MONTH FROM d) IN (1, 11, 12) THEN TRUE ELSE FALSE END AS is_peak_season
            FROM GENERATE_SERIES('2024-01-01'::DATE, '2028-12-31'::DATE, '1 day'::INTERVAL) AS d
            ON CONFLICT (date_key) DO NOTHING;
        """))

        # 2. Nạp Dim_Hub (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_hub (hub_nk, hub_name, hub_level, city, district, capacity, is_current)
            SELECT hub_code, hub_name, hub_level, city, district, capacity, TRUE
            FROM silver.hubs
            WHERE batch_id = :batch_id
            ON CONFLICT (hub_nk) DO UPDATE SET
                hub_name = EXCLUDED.hub_name,
                hub_level = EXCLUDED.hub_level,
                city = EXCLUDED.city,
                district = EXCLUDED.district,
                capacity = EXCLUDED.capacity;
        """), {"batch_id": batch_id})

        # 3. Nạp Dim_Vehicle (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_vehicle (vehicle_nk, vehicle_type, capacity_kg)
            SELECT vehicle_code, vehicle_type, capacity_kg
            FROM silver.vehicles
            WHERE batch_id = :batch_id
            ON CONFLICT (vehicle_nk) DO UPDATE SET
                vehicle_type = EXCLUDED.vehicle_type,
                capacity_kg = EXCLUDED.capacity_kg;
        """), {"batch_id": batch_id})

        # 4. Nạp Dim_Service_Type (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_service_type (service_type_nk, service_name, sla_hours, is_express)
            SELECT service_code, service_name, sla_hours, is_express
            FROM silver.service_types
            WHERE batch_id = :batch_id
            ON CONFLICT (service_type_nk) DO UPDATE SET
                service_name = EXCLUDED.service_name,
                sla_hours = EXCLUDED.sla_hours,
                is_express = EXCLUDED.is_express;
        """), {"batch_id": batch_id})

        # 5. Nạp Dim_Status_Reason (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_status_reason (status_code, status_name, reason_code, reason_name, event_category)
            SELECT status_code, status_name, reason_code, reason_name, event_category
            FROM silver.status_reasons
            WHERE batch_id = :batch_id
            ON CONFLICT (reason_code) DO UPDATE SET
                status_code = EXCLUDED.status_code,
                status_name = EXCLUDED.status_name,
                reason_name = EXCLUDED.reason_name,
                event_category = EXCLUDED.event_category;
        """), {"batch_id": batch_id})

        # 6. Nạp Dim_Payment_Method (SCD Type 1)
        conn.execute(text("""
            INSERT INTO gold.dim_payment_method (payment_method_nk, payment_method_name, payer_type, has_cod)
            SELECT payment_method_code, payment_method_name, payer_type, has_cod
            FROM silver.payment_methods
            WHERE batch_id = :batch_id
            ON CONFLICT (payment_method_nk) DO UPDATE SET
                payment_method_name = EXCLUDED.payment_method_name,
                payer_type = EXCLUDED.payer_type,
                has_cod = EXCLUDED.has_cod;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 7. NẠP DIM_CUSTOMER (SCD TYPE 2 BẰNG ROW_HASH)
        # =====================================================================
        # Bước 7.1: Đóng các bản ghi hiện hành nếu row_hash thay đổi
        conn.execute(text("""
            UPDATE gold.dim_customer d
            SET is_current = FALSE,
                valid_to = s.updated_at
            FROM silver.customers s
            WHERE s.batch_id = :batch_id
              AND d.customer_nk = s.customer_code
              AND d.is_current = TRUE
              AND d.row_hash <> s.row_hash;
        """), {"batch_id": batch_id})

        # Bước 7.2: Insert khách hàng mới hoàn toàn (valid_from = '1970-01-01' để cover toàn bộ lịch sử đơn cũ)
        # hoặc khách hàng vừa đổi hạng (valid_from = s.updated_at)
        conn.execute(text("""
            INSERT INTO gold.dim_customer (
                customer_nk, customer_name, customer_type, membership_tier,
                city, district, valid_from, valid_to, is_current, row_hash
            )
            SELECT
                s.customer_code,
                s.customer_name,
                s.customer_type,
                s.membership_tier,
                s.city,
                s.district,
                CASE
                    WHEN EXISTS (SELECT 1 FROM gold.dim_customer prev WHERE prev.customer_nk = s.customer_code)
                    THEN s.updated_at
                    ELSE TIMESTAMP '1970-01-01 00:00:00'
                END AS valid_from,
                TIMESTAMP '9999-12-31 23:59:59' AS valid_to,
                TRUE AS is_current,
                s.row_hash
            FROM silver.customers s
            LEFT JOIN gold.dim_customer d
              ON d.customer_nk = s.customer_code AND d.is_current = TRUE
            WHERE s.batch_id = :batch_id
              AND d.customer_sk IS NULL;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 8. NẠP DIM_COURIER (SCD TYPE 2 BẰNG ROW_HASH - PHỤ THUỘC DIM_HUB & DIM_VEHICLE)
        # =====================================================================
        conn.execute(text("""
            UPDATE gold.dim_courier d
            SET is_current = FALSE,
                valid_to = s.updated_at
            FROM silver.couriers s
            WHERE s.batch_id = :batch_id
              AND d.courier_nk = s.courier_code
              AND d.is_current = TRUE
              AND d.row_hash <> s.row_hash;
        """), {"batch_id": batch_id})

        conn.execute(text("""
            INSERT INTO gold.dim_courier (
                courier_nk, courier_name, hub_sk, contract_type,
                vehicle_sk, valid_from, valid_to, is_current, row_hash
            )
            SELECT
                s.courier_code,
                s.courier_name,
                COALESCE(dh.hub_sk, -1) AS hub_sk,
                s.contract_type,
                COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
                CASE
                    WHEN EXISTS (SELECT 1 FROM gold.dim_courier prev WHERE prev.courier_nk = s.courier_code)
                    THEN s.updated_at
                    ELSE TIMESTAMP '1970-01-01 00:00:00'
                END AS valid_from,
                TIMESTAMP '9999-12-31 23:59:59' AS valid_to,
                TRUE AS is_current,
                s.row_hash
            FROM silver.couriers s
            LEFT JOIN silver.hubs sh ON s.hub_id = sh.hub_id
            LEFT JOIN gold.dim_hub dh ON sh.hub_code = dh.hub_nk
            LEFT JOIN silver.vehicles sv ON s.vehicle_id = sv.vehicle_id
            LEFT JOIN gold.dim_vehicle dv ON sv.vehicle_code = dv.vehicle_nk
            LEFT JOIN gold.dim_courier d
              ON d.courier_nk = s.courier_code AND d.is_current = TRUE
            WHERE s.batch_id = :batch_id
              AND d.courier_sk IS NULL;
        """), {"batch_id": batch_id})

        conn.commit()
        print("✅ Đã nạp hoàn tất 8 bảng Dimensions!")
```

### File: `scripts/load_facts.py`

```python
from sqlalchemy import text
from config.db_config import get_dwh_conn

def run_load_facts(batch_id: str):
    print(f"📊 [Batch: {batch_id}] Đang nạp 3 bảng Facts (SCD2 Point-in-Time Join & Idempotent Upsert)...")

    with get_dwh_conn() as conn:
        # =====================================================================
        # 1. NẠP FACT_SHIPMENT_LIFECYCLE (ACCUMULATING SNAPSHOT - UPSERT)
        # Join SCD2 với Dim_Customer theo đúng thời điểm nghiệp vụ order_date
        # =====================================================================
        conn.execute(text("""
            WITH impacted_shipments AS (
                SELECT shipment_id FROM silver.shipments WHERE batch_id = :batch_id
                UNION
                SELECT DISTINCT shipment_id FROM silver.shipment_tracking_events WHERE batch_id = :batch_id
            ),
            event_milestones AS (
                SELECT
                    e.shipment_id,
                    MIN(CASE WHEN e.event_type = 'INBOUND_SCAN' THEN e.event_time END) AS first_sorting_time,
                    MAX(CASE WHEN e.event_type = 'OUTBOUND_SCAN' THEN e.event_time END) AS last_sorting_time,
                    MIN(CASE WHEN e.event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED', 'RETURNED') THEN e.event_time END) AS first_attempt_time,
                    MAX(CASE WHEN e.event_type = 'RETURNED' THEN e.event_time END) AS return_time
                FROM silver.shipment_tracking_events e
                JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
                GROUP BY e.shipment_id
            ),
            status_lookup AS (
                SELECT DISTINCT ON (status_code)
                    status_code, status_reason_sk
                FROM gold.dim_status_reason
                ORDER BY status_code, status_reason_sk
            )
            INSERT INTO gold.fact_shipment_lifecycle (
                shipment_number,
                sender_customer_sk, receiver_customer_sk,
                origin_hub_sk, destination_hub_sk,
                service_type_sk, payment_method_sk, current_status_sk,
                order_date_key, pickup_date_key, first_sorting_date_key, last_sorting_date_key,
                first_delivery_attempt_date_key, final_delivery_date_key, return_date_key, promised_delivery_date_key,
                weight, freight_charge, cod_amount, distance_km,
                pickup_to_sorting_hours, sorting_to_delivery_hours, total_lead_time_hours,
                is_delivered, is_returned, is_cancelled, is_on_time, is_sla_breached,
                created_at, updated_at, batch_id
            )
            SELECT
                s.shipment_number,
                COALESCE(dc_send.customer_sk, -1) AS sender_customer_sk,
                COALESCE(dc_recv.customer_sk, -1) AS receiver_customer_sk,
                COALESCE(dh_orig.hub_sk, -1) AS origin_hub_sk,
                COALESCE(dh_dest.hub_sk, -1) AS destination_hub_sk,
                COALESCE(dst.service_type_sk, -1) AS service_type_sk,
                COALESCE(dpm.payment_method_sk, -1) AS payment_method_sk,
                COALESCE(dsl.status_reason_sk, -1) AS current_status_sk,

                COALESCE(TO_CHAR(s.order_date, 'YYYYMMDD')::INT, -1) AS order_date_key,
                COALESCE(TO_CHAR(s.pickup_date, 'YYYYMMDD')::INT, -1) AS pickup_date_key,
                COALESCE(TO_CHAR(m.first_sorting_time, 'YYYYMMDD')::INT, -1) AS first_sorting_date_key,
                COALESCE(TO_CHAR(m.last_sorting_time, 'YYYYMMDD')::INT, -1) AS last_sorting_date_key,
                COALESCE(TO_CHAR(m.first_attempt_time, 'YYYYMMDD')::INT, -1) AS first_delivery_attempt_date_key,
                CASE WHEN s.current_status = 'DELIVERED' THEN COALESCE(TO_CHAR(s.actual_delivery_date, 'YYYYMMDD')::INT, -1) ELSE -1 END AS final_delivery_date_key,
                CASE WHEN s.current_status = 'RETURNED' THEN COALESCE(TO_CHAR(COALESCE(m.return_time, s.actual_delivery_date), 'YYYYMMDD')::INT, -1) ELSE -1 END AS return_date_key,
                COALESCE(TO_CHAR(s.promised_delivery_date, 'YYYYMMDD')::INT, -1) AS promised_delivery_date_key,

                s.weight,
                s.freight_charge,
                s.cod_amount,
                s.distance_km,

                ROUND((EXTRACT(EPOCH FROM (m.first_sorting_time - s.pickup_date)) / 3600.0)::NUMERIC, 2) AS pickup_to_sorting_hours,
                ROUND((EXTRACT(EPOCH FROM (s.actual_delivery_date - m.first_sorting_time)) / 3600.0)::NUMERIC, 2) AS sorting_to_delivery_hours,
                ROUND((EXTRACT(EPOCH FROM (s.actual_delivery_date - s.order_date)) / 3600.0)::NUMERIC, 2) AS total_lead_time_hours,

                (s.current_status = 'DELIVERED') AS is_delivered,
                (s.current_status = 'RETURNED') AS is_returned,
                (s.current_status = 'CANCELLED') AS is_cancelled,
                CASE
                    WHEN s.current_status = 'DELIVERED' AND s.actual_delivery_date <= s.promised_delivery_date THEN TRUE
                    ELSE FALSE
                END AS is_on_time,
                CASE
                    WHEN s.actual_delivery_date IS NOT NULL AND s.actual_delivery_date > s.promised_delivery_date THEN TRUE
                    ELSE FALSE
                END AS is_sla_breached,
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP,
                :batch_id
            FROM silver.shipments s
            JOIN impacted_shipments imp ON s.shipment_id = imp.shipment_id
            LEFT JOIN event_milestones m ON s.shipment_id = m.shipment_id
            LEFT JOIN silver.customers sc_send ON s.sender_customer_id = sc_send.customer_id
            -- Join SCD2 theo đúng thời điểm đặt hàng (Point-in-Time Join)
            LEFT JOIN LATERAL (
                SELECT customer_sk FROM gold.dim_customer c
                WHERE c.customer_nk = sc_send.customer_code
                  AND s.order_date >= c.valid_from AND s.order_date < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dc_send ON TRUE
            LEFT JOIN silver.customers sc_recv ON s.receiver_customer_id = sc_recv.customer_id
            LEFT JOIN LATERAL (
                SELECT customer_sk FROM gold.dim_customer c
                WHERE c.customer_nk = sc_recv.customer_code
                  AND s.order_date >= c.valid_from AND s.order_date < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dc_recv ON TRUE
            LEFT JOIN silver.hubs sh_orig ON s.origin_hub_id = sh_orig.hub_id
            LEFT JOIN gold.dim_hub dh_orig ON sh_orig.hub_code = dh_orig.hub_nk
            LEFT JOIN silver.hubs sh_dest ON s.destination_hub_id = sh_dest.hub_id
            LEFT JOIN gold.dim_hub dh_dest ON sh_dest.hub_code = dh_dest.hub_nk
            LEFT JOIN silver.service_types sst ON s.service_type_id = sst.service_type_id
            LEFT JOIN gold.dim_service_type dst ON sst.service_code = dst.service_type_nk
            LEFT JOIN silver.payment_methods spm ON s.payment_method_id = spm.payment_method_id
            LEFT JOIN gold.dim_payment_method dpm ON spm.payment_method_code = dpm.payment_method_nk
            LEFT JOIN status_lookup dsl ON s.current_status = dsl.status_code
            ON CONFLICT (shipment_number) DO UPDATE SET
                current_status_sk = EXCLUDED.current_status_sk,
                pickup_date_key = EXCLUDED.pickup_date_key,
                first_sorting_date_key = EXCLUDED.first_sorting_date_key,
                last_sorting_date_key = EXCLUDED.last_sorting_date_key,
                first_delivery_attempt_date_key = EXCLUDED.first_delivery_attempt_date_key,
                final_delivery_date_key = EXCLUDED.final_delivery_date_key,
                return_date_key = EXCLUDED.return_date_key,
                pickup_to_sorting_hours = EXCLUDED.pickup_to_sorting_hours,
                sorting_to_delivery_hours = EXCLUDED.sorting_to_delivery_hours,
                total_lead_time_hours = EXCLUDED.total_lead_time_hours,
                is_delivered = EXCLUDED.is_delivered,
                is_returned = EXCLUDED.is_returned,
                is_cancelled = EXCLUDED.is_cancelled,
                is_on_time = EXCLUDED.is_on_time,
                is_sla_breached = EXCLUDED.is_sla_breached,
                updated_at = CURRENT_TIMESTAMP,
                batch_id = EXCLUDED.batch_id;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 2. NẠP FACT_TRACKING_EVENTS (TRANSACTION FACT - UPSERT THEO EVENT_ID_NK)
        # Join SCD2 với Dim_Courier theo đúng thời điểm quét event_time
        # =====================================================================
        conn.execute(text("""
            WITH seq_events AS (
                SELECT
                    e.*,
                    ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_time, e.event_id) AS event_seq
                FROM silver.shipment_tracking_events e
                WHERE e.shipment_id IN (
                    SELECT DISTINCT shipment_id FROM silver.shipment_tracking_events WHERE batch_id = :batch_id
                )
            )
            INSERT INTO gold.fact_tracking_events (
                event_id_nk, shipment_sk, event_date_key, event_timestamp,
                event_type, hub_sk, courier_sk, vehicle_sk, status_reason_sk,
                event_sequence, scan_source, batch_id, loaded_at
            )
            SELECT
                e.event_id AS event_id_nk,
                fsl.shipment_sk,
                COALESCE(TO_CHAR(e.event_time, 'YYYYMMDD')::INT, -1) AS event_date_key,
                e.event_time AS event_timestamp,
                e.event_type,
                COALESCE(dh.hub_sk, -1) AS hub_sk,
                COALESCE(dco.courier_sk, -1) AS courier_sk,
                COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
                COALESCE(dsr.status_reason_sk, -1) AS status_reason_sk,
                e.event_seq AS event_sequence,
                e.scan_source,
                :batch_id,
                CURRENT_TIMESTAMP
            FROM seq_events e
            JOIN silver.shipments ss ON e.shipment_id = ss.shipment_id
            JOIN gold.fact_shipment_lifecycle fsl ON ss.shipment_number = fsl.shipment_number
            LEFT JOIN silver.hubs sh ON e.hub_id = sh.hub_id
            LEFT JOIN gold.dim_hub dh ON sh.hub_code = dh.hub_nk
            LEFT JOIN silver.couriers sco ON e.courier_id = sco.courier_id
            -- Join SCD2 Dim_Courier theo thời điểm phát sinh sự kiện (event_time)
            LEFT JOIN LATERAL (
                SELECT courier_sk FROM gold.dim_courier c
                WHERE c.courier_nk = sco.courier_code
                  AND e.event_time >= c.valid_from AND e.event_time < c.valid_to
                ORDER BY c.valid_from DESC LIMIT 1
            ) dco ON TRUE
            LEFT JOIN silver.vehicles sv ON e.vehicle_id = sv.vehicle_id
            LEFT JOIN gold.dim_vehicle dv ON sv.vehicle_code = dv.vehicle_nk
            LEFT JOIN silver.status_reasons ssr ON e.status_reason_id = ssr.status_reason_id
            LEFT JOIN gold.dim_status_reason dsr ON ssr.reason_code = dsr.reason_code
            WHERE e.batch_id = :batch_id
            ON CONFLICT (event_id_nk) DO UPDATE SET
                event_date_key = EXCLUDED.event_date_key,
                event_timestamp = EXCLUDED.event_timestamp,
                event_type = EXCLUDED.event_type,
                hub_sk = EXCLUDED.hub_sk,
                courier_sk = EXCLUDED.courier_sk,
                vehicle_sk = EXCLUDED.vehicle_sk,
                status_reason_sk = EXCLUDED.status_reason_sk,
                event_sequence = EXCLUDED.event_sequence,
                batch_id = EXCLUDED.batch_id,
                loaded_at = CURRENT_TIMESTAMP;
        """), {"batch_id": batch_id})

        # =====================================================================
        # 3. NẠP FACT_DAILY_HUB_SNAPSHOT (PERIODIC SNAPSHOT - IDEMPOTENT UPSERT)
        # =====================================================================
        conn.execute(text("""
            WITH active_dates AS (
                SELECT DISTINCT event_date_key AS snapshot_date_key
                FROM gold.fact_tracking_events
                WHERE batch_id = :batch_id AND event_date_key <> -1
            ),
            hub_daily_flow AS (
                SELECT
                    e.event_date_key AS snapshot_date_key,
                    e.hub_sk,
                    COUNT(CASE WHEN e.event_type = 'INBOUND_SCAN' THEN 1 END) AS inbound_count,
                    COUNT(CASE WHEN e.event_type = 'OUTBOUND_SCAN' THEN 1 END) AS outbound_count,
                    COUNT(CASE WHEN e.event_type = 'DELIVERY_ATTEMPT' THEN 1 END) AS pending_delivery_count,
                    COUNT(CASE WHEN e.event_type = 'RETURNED' THEN 1 END) AS return_count
                FROM gold.fact_tracking_events e
                JOIN active_dates ad ON e.event_date_key = ad.snapshot_date_key
                WHERE e.hub_sk <> -1
                GROUP BY e.event_date_key, e.hub_sk
            ),
            hub_backlog AS (
                -- Các đơn đã INBOUND vào Hub tính đến cuối ngày snapshot nhưng chưa OUTBOUND hoặc DELIVERED
                SELECT
                    ad.snapshot_date_key,
                    in_ev.hub_sk,
                    COUNT(DISTINCT in_ev.shipment_sk) AS backlog_count,
                    COALESCE(SUM(DISTINCT CASE WHEN cs.settlement_status = 'PENDING' THEN fsl.cod_amount ELSE 0 END), 0) AS cod_held_amount
                FROM active_dates ad
                JOIN gold.fact_tracking_events in_ev
                  ON in_ev.event_date_key <= ad.snapshot_date_key
                 AND in_ev.event_type = 'INBOUND_SCAN'
                 AND in_ev.hub_sk <> -1
                JOIN gold.fact_shipment_lifecycle fsl ON in_ev.shipment_sk = fsl.shipment_sk
                LEFT JOIN silver.shipments ss ON fsl.shipment_number = ss.shipment_number
                LEFT JOIN silver.cod_settlements cs ON ss.shipment_id = cs.shipment_id
                WHERE NOT EXISTS (
                    SELECT 1 FROM gold.fact_tracking_events out_ev
                    WHERE out_ev.shipment_sk = in_ev.shipment_sk
                      AND out_ev.hub_sk = in_ev.hub_sk
                      AND out_ev.event_type IN ('OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
                      AND out_ev.event_date_key <= ad.snapshot_date_key
                      AND out_ev.event_timestamp >= in_ev.event_timestamp
                )
                GROUP BY ad.snapshot_date_key, in_ev.hub_sk
            )
            INSERT INTO gold.fact_daily_hub_snapshot (
                snapshot_date_key, hub_sk,
                inbound_count, outbound_count, backlog_count,
                pending_delivery_count, return_count, cod_held_amount,
                batch_id, snapshot_generated_at
            )
            SELECT
                COALESCE(f.snapshot_date_key, b.snapshot_date_key) AS snapshot_date_key,
                COALESCE(f.hub_sk, b.hub_sk) AS hub_sk,
                COALESCE(f.inbound_count, 0) AS inbound_count,
                COALESCE(f.outbound_count, 0) AS outbound_count,
                COALESCE(b.backlog_count, 0) AS backlog_count,
                COALESCE(f.pending_delivery_count, 0) AS pending_delivery_count,
                COALESCE(f.return_count, 0) AS return_count,
                COALESCE(b.cod_held_amount, 0) AS cod_held_amount,
                :batch_id,
                CURRENT_TIMESTAMP
            FROM hub_daily_flow f
            FULL OUTER JOIN hub_backlog b
              ON f.snapshot_date_key = b.snapshot_date_key AND f.hub_sk = b.hub_sk
            ON CONFLICT (snapshot_date_key, hub_sk) DO UPDATE SET
                inbound_count = EXCLUDED.inbound_count,
                outbound_count = EXCLUDED.outbound_count,
                backlog_count = EXCLUDED.backlog_count,
                pending_delivery_count = EXCLUDED.pending_delivery_count,
                return_count = EXCLUDED.return_count,
                cod_held_amount = EXCLUDED.cod_held_amount,
                batch_id = EXCLUDED.batch_id,
                snapshot_generated_at = CURRENT_TIMESTAMP;
        """), {"batch_id": batch_id})

        conn.commit()
        print("✅ Đã nạp hoàn tất 3 bảng Facts!")
```

### File: `scripts/refresh_views.py`

```python
from sqlalchemy import text
from config.db_config import get_dwh_conn
from scripts.extract_incremental import TABLES_TO_EXTRACT

def run_post_load_and_refresh_views(batch_id: str):
    print(f"🔄 [Batch: {batch_id}] Refresh Materialized Views & Cập nhật Watermark / Audit Log...")

    with get_dwh_conn() as conn:
        # 1. Refresh các Materialized Views trong tầng Gold
        conn.execute(text("REFRESH MATERIALIZED VIEW gold.mv_daily_hub_performance;"))
        conn.execute(text("REFRESH MATERIALIZED VIEW gold.mv_courier_performance;"))

        # 2. Cập nhật Watermark và hoàn tất Batch Log cho từng bảng
        for table in TABLES_TO_EXTRACT:
            conn.execute(text(f"""
                UPDATE meta.etl_watermark
                SET last_extracted_timestamp = COALESCE(
                        (SELECT MAX(updated_at) FROM stg.{table} WHERE batch_id = :batch_id),
                        last_extracted_timestamp
                    ),
                    last_batch_id = :batch_id,
                    updated_at = CURRENT_TIMESTAMP
                WHERE table_name = :table_name;
            """), {"batch_id": batch_id, "table_name": table})

            conn.execute(text(f"""
                UPDATE meta.etl_batch_log
                SET run_end = CURRENT_TIMESTAMP,
                    rows_loaded = GREATEST(rows_extracted - rows_rejected, 0),
                    status = 'SUCCESS'
                WHERE batch_id = :batch_id AND table_name = :table_name;
            """), {"batch_id": batch_id, "table_name": table})

        conn.commit()
        print("✅ Đã refresh Materialized Views và cập nhật Watermark thành công!")
```

### File: `scripts/run_local_pipeline.py`

```python
import uuid
from pathlib import Path
from sqlalchemy import text
from config.db_config import get_oltp_conn, get_dwh_conn
from scripts.generate_oltp_data import generate_data
from scripts.extract_incremental import run_extract_incremental
from scripts.data_quality_checks import run_data_quality_and_silver_load
from scripts.load_dimensions import run_load_dimensions
from scripts.load_facts import run_load_facts
from scripts.refresh_views import run_post_load_and_refresh_views

BASE_DIR = Path(__file__).resolve().parent.parent

def execute_sql_file(conn, file_path: Path):
    sql_content = file_path.read_text(encoding="utf-8")
    conn.execute(text(sql_content))
    conn.commit()

def init_databases():
    print("🛠️ Đang khởi tạo DDL cho OLTP và DWH...")
    with get_oltp_conn() as oltp_conn:
        execute_sql_file(oltp_conn, BASE_DIR / "sql/oltp/01_create_oltp_tables.sql")
        execute_sql_file(oltp_conn, BASE_DIR / "sql/oltp/02_seed_constraints.sql")

    with get_dwh_conn() as dwh_conn:
        for sql_file in sorted((BASE_DIR / "sql/dwh").glob("*.sql")):
            execute_sql_file(dwh_conn, sql_file)
        for sql_file in sorted((BASE_DIR / "sql/analytics").glob("*.sql")):
            execute_sql_file(dwh_conn, sql_file)

def run_pipeline():
    batch_id = f"BATCH_{uuid.uuid4().hex[:10].upper()}"
    print("=" * 75)
    print(f"🚀 KHỞI CHẠY EXPRESS DELIVERY DWH PIPELINE | BATCH_ID: {batch_id}")
    print("=" * 75)

    init_databases()
    generate_data()
    run_extract_incremental(batch_id)
    run_data_quality_and_silver_load(batch_id)
    run_load_dimensions(batch_id)
    run_load_facts(batch_id)
    run_post_load_and_refresh_views(batch_id)

    # In báo cáo tổng kết
    with get_dwh_conn() as conn:
        fsl_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_shipment_lifecycle")).scalar()
        fte_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_tracking_events")).scalar()
        fdhs_cnt = conn.execute(text("SELECT COUNT(*) FROM gold.fact_daily_hub_snapshot")).scalar()
        quar_cnt = conn.execute(text("SELECT COUNT(*) FROM meta.data_quarantine")).scalar()
        print("=" * 75)
        print(f"🎉 PIPELINE HOÀN TẤT THÀNH CÔNG!")
        print(f"   • Fact_Shipment_Lifecycle : {fsl_cnt:,} dòng")
        print(f"   • Fact_Tracking_Events    : {fte_cnt:,} dòng")
        print(f"   • Fact_Daily_Hub_Snapshot : {fdhs_cnt:,} dòng")
        print(f"   • Meta.Data_Quarantine    : {quar_cnt:,} bản ghi lỗi/cảnh báo đã bắt được")
        print("=" * 75)

if __name__ == "__main__":
    run_pipeline()

```

---

## 7. Đóng gói Orchestration bằng Apache Airflow (`dags/`)

### File: `dags/express_delivery_dwh_dag.py`

```python
import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import text

# Đảm bảo Airflow import được các module trong project
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.db_config import get_oltp_conn, get_dwh_conn
from scripts.extract_incremental import run_extract_incremental
from scripts.data_quality_checks import run_data_quality_and_silver_load
from scripts.load_dimensions import run_load_dimensions
from scripts.load_facts import run_load_facts
from scripts.refresh_views import run_post_load_and_refresh_views

def task_preflight_checks(**context):
    with get_oltp_conn() as oltp_conn:
        oltp_conn.execute(text("SELECT 1 FROM shipments LIMIT 1"))
    with get_dwh_conn() as dwh_conn:
        dwh_conn.execute(text("SELECT 1 FROM meta.etl_watermark LIMIT 1"))
    print("✅ Preflight checks: Kết nối OLTP, DWH và các bảng Meta đều sẵn sàng.")

def task_extract_incremental(**context):
    batch_id = f"AF_{context['run_id']}"
    run_extract_incremental(batch_id)

def task_data_quality_gate(**context):
    batch_id = f"AF_{context['run_id']}"
    run_data_quality_and_silver_load(batch_id)

def task_load_dimensions(**context):
    batch_id = f"AF_{context['run_id']}"
    run_load_dimensions(batch_id)

def task_load_facts(**context):
    batch_id = f"AF_{context['run_id']}"
    run_load_facts(batch_id)

def task_refresh_views_and_watermark(**context):
    batch_id = f"AF_{context['run_id']}"
    run_post_load_and_refresh_views(batch_id)

default_args = {
    "owner": "express_dwh_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="express_delivery_dwh_dag",
    default_args=default_args,
    description="Enterprise Medallion + Kimball ETL Pipeline cho Express Delivery DWH",
    schedule_interval="0 */4 * * *",
    catchup=False,
    tags=["express_delivery", "medallion", "kimball", "scd2"]
) as dag:

    preflight_checks = PythonOperator(
        task_id="preflight_checks",
        python_callable=task_preflight_checks
    )

    extract_incremental = PythonOperator(
        task_id="extract_incremental",
        python_callable=task_extract_incremental
    )

    data_quality_gate = PythonOperator(
        task_id="data_quality_gate",
        python_callable=task_data_quality_gate
    )

    load_dimensions = PythonOperator(
        task_id="load_dimensions",
        python_callable=task_load_dimensions
    )

    load_facts = PythonOperator(
        task_id="load_facts",
        python_callable=task_load_facts
    )

    refresh_views = PythonOperator(
        task_id="refresh_views_and_update_watermark",
        python_callable=task_refresh_views_and_watermark
    )

    preflight_checks >> extract_incremental >> data_quality_gate >> load_dimensions >> load_facts >> refresh_views
```

---

## 8. Đóng gói Docker Compose (`docker/`)

### File: `docker/requirements.txt`

```text
SQLAlchemy==2.0.30
psycopg2-binary==2.9.9
pandas==2.2.2
Faker==25.2.0
```

### File: `docker/docker-compose.yml`

```yaml
version: '3.8'

services:
  postgres-db:
    image: postgres:15
    container_name: express_postgres
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=airflow_meta
    ports:
      - "5432:5432"
    volumes:
      - pg_dwh_data:/var/lib/postgresql/data
    command: >
      bash -c "
      docker-entrypoint.sh postgres &
      sleep 5;
      psql -U postgres -tc \"SELECT 1 FROM pg_database WHERE datname = 'express_oltp'\" | grep -q 1 || psql -U postgres -c 'CREATE DATABASE express_oltp';
      psql -U postgres -tc \"SELECT 1 FROM pg_database WHERE datname = 'express_dwh'\" | grep -q 1 || psql -U postgres -c 'CREATE DATABASE express_dwh';
      wait
      "
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  airflow:
    image: apache/airflow:2.9.1-python3.11
    container_name: express_airflow
    depends_on:
      postgres-db:
        condition: service_healthy
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres-db:5432/airflow_meta
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - OLTP_DB_URI=postgresql+psycopg2://postgres:postgres@postgres-db:5432/express_oltp
      - DWH_DB_URI=postgresql+psycopg2://postgres:postgres@postgres-db:5432/express_dwh
      - PYTHONPATH=/opt/airflow   # FIX LỖI 2: Báo cho Python biết thư mục gốc của project để import 'config'
    volumes:
      - ../dags:/opt/airflow/dags
      - ../scripts:/opt/airflow/scripts
      - ../config:/opt/airflow/config
      - ../sql:/opt/airflow/sql
    ports:
      - "8080:8080"
    command: >
      bash -c "
      pip install 'SQLAlchemy>=1.4.49,<2.0' psycopg2-binary==2.9.9 pandas==2.1.4 Faker==25.2.0 &&
      export PYTHONPATH=/opt/airflow &&
      airflow db migrate &&
      airflow users create --username admin --password admin --firstname Express --lastname Admin --role Admin --email admin@express.vn || true &&
      python /opt/airflow/scripts/run_local_pipeline.py &&
      (airflow webserver & airflow scheduler)
      "

volumes:
  pg_dwh_data:
```

---

## 9. Hướng dẫn chạy thực tế (Local & Docker) và Kiểm thử khi bảo vệ

### Cách 1: Chạy trực tiếp bằng Docker Compose (Tự động 100%)

```bash
cd docker
docker-compose up -d
```

* Container `express_postgres` sẽ tự tạo 2 database `express_oltp` và `express_dwh`.
* Container `express_airflow` sẽ tự động chạy `run_local_pipeline.py` để khởi tạo toàn bộ bảng, sinh **10,000 Shipments + ~45,000 Tracking Events**, chạy ETL lần 1 và mở Airflow UI tại `http://localhost:8080` (`admin` / `admin`).
* Để demo **Incremental Load + SCD Type 2 + Upsert Accumulating Fact**, bạn chỉ cần chạy lệnh:

```bash
docker exec -it express_airflow python /opt/airflow/scripts/run_local_pipeline.py
```

Lần chạy thứ 2 này sẽ tự động kích hoạt hàm `simulate_scd2_and_incremental_updates()` (đổi hạng 25 khách hàng, chuyển Hub 15 shipper, hoàn tất giao 50 đơn hàng `IN_TRANSIT`) và chỉ nạp phần dữ liệu thay đổi vào DWH.

### Cách 2: Truy vấn SQL kiểm chứng để trình chiếu khi bảo vệ đồ án

```sql
-- 1. Kiểm tra Audit Log và Watermark sau các lần chạy
SELECT * FROM meta.etl_watermark;
SELECT batch_id, table_name, rows_extracted, rows_loaded, rows_rejected, status FROM meta.etl_batch_log ORDER BY run_start DESC;

-- 2. Kiểm tra các bản ghi bẩn bị chặn tại cổng Data Quality Gate
SELECT source_table, rule_name, severity, COUNT(*) AS total_violations
FROM meta.data_quarantine
GROUP BY source_table, rule_name, severity;

-- 3. Chứng minh SCD Type 2 lưu giữ lịch sử đổi hạng của Khách hàng và chuyển Hub của Shipper
SELECT customer_nk, customer_name, membership_tier, valid_from, valid_to, is_current
FROM gold.dim_customer
WHERE customer_nk IN (SELECT customer_nk FROM gold.dim_customer GROUP BY customer_nk HAVING COUNT(*) > 1)
ORDER BY customer_nk, valid_from;
```
