Dưới đây là bản tài liệu triển khai đầy đủ cho kiến trúc mới (**PostgreSQL OLTP $\rightarrow$ Apache Airflow + Snowflake Data Cloud + dbt + Docker**)

## 1. Cấu trúc thư mục chuẩn của dự án mới

```text
express-delivery-dwh/
├── config/
│   ├── db_config.py
│   └── settings.py
├── sql/
│   ├── oltp/
│   │   ├── 01_create_oltp_tables.sql
│   │   └── 02_seed_constraints.sql
│   └── snowflake_init/
│       └── 01_init_snowflake.sql
├── scripts/
│   ├── generate_oltp_data.py
│   ├── extract_load_snowflake.py
│   ├── dq_gate_snowflake.py
│   ├── post_load_watermark.py
│   └── run_local_pipeline.py
├── dbt_project/
│   ├── dbt_project.yml
│   ├── packages.yml
│   ├── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── snapshots/
│   │   ├── snap_customers.sql
│   │   └── snap_couriers.sql
│   └── models/
│       ├── sources.yml
│       ├── schema.yml
│       ├── silver/
│       │   ├── silver_customers.sql
│       │   ├── silver_hubs.sql
│       │   ├── silver_vehicles.sql
│       │   ├── silver_couriers.sql
│       │   ├── silver_service_types.sql
│       │   ├── silver_payment_methods.sql
│       │   ├── silver_status_reasons.sql
│       │   ├── silver_shipments.sql
│       │   ├── silver_shipment_tracking_events.sql
│       │   ├── silver_cod_settlements.sql
│       │   └── silver_customer_tickets.sql
│       └── gold/
│           ├── dimensions/
│           │   ├── dim_date.sql
│           │   ├── dim_hub.sql
│           │   ├── dim_vehicle.sql
│           │   ├── dim_service_type.sql
│           │   ├── dim_status_reason.sql
│           │   ├── dim_payment_method.sql
│           │   ├── dim_customer.sql
│           │   └── dim_courier.sql
│           ├── facts/
│           │   ├── fact_shipment_lifecycle.sql
│           │   ├── fact_tracking_events.sql
│           │   └── fact_daily_hub_snapshot.sql
│           └── analytics/
│               ├── v_slaship_summary.sql
│               ├── mv_daily_hub_performance.sql
│               └── mv_courier_performance.sql
├── dags/
│   └── express_delivery_snowflake_dbt_dag.py
└── docker/
    ├── docker-compose.yml
    ├── init-db.sql
    └── requirements.txt
```

---

## 2. Nhóm Cấu hình (`config/`)

### File: `config/settings.py`

```python
import os

# 1. Kết nối PostgreSQL OLTP (Nguồn giao dịch)
OLTP_DB_URI = os.getenv(
    "OLTP_DB_URI",
    "postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
)

# 2. Kết nối Snowflake Data Cloud (Kho dữ liệu đích)
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "your_account_locator")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "your_username")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "your_password")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "EXPRESS_WH")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "EXPRESS_DWH")

# 3. Quy mô sinh dữ liệu OLTP mặc định (Giữ nguyên từ bản cũ)
DEFAULT_CUSTOMERS = 1500
DEFAULT_HUBS = 30
DEFAULT_COURIERS = 200
DEFAULT_VEHICLES = 80
DEFAULT_SHIPMENTS = 10000
SIMULATE_NEW_SHIPMENTS = 200
```

### File: `config/db_config.py`

```python
from contextlib import contextmanager
from sqlalchemy import create_engine
import snowflake.connector
from config.settings import (
    OLTP_DB_URI,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
)

oltp_engine = create_engine(OLTP_DB_URI, pool_pre_ping=True, future=True)


@contextmanager
def get_oltp_conn():
    conn = oltp_engine.connect()
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_snowflake_conn(schema: str = "META"):
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=schema,
        session_parameters={"TIMEZONE": "Asia/Ho_Chi_Minh"},
    )
    try:
        yield conn
    finally:
        conn.close()
```

---

## 3. Nhóm SQL Nguồn OLTP (`sql/oltp/`) & Sinh dữ liệu (`scripts/generate_oltp_data.py`)

### File: `sql/oltp/01_create_oltp_tables.sql`

```sql
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
```

### File: `sql/oltp/02_seed_constraints.sql`

```sql
CREATE INDEX IF NOT EXISTS idx_customers_updated_at ON customers(updated_at);
CREATE INDEX IF NOT EXISTS idx_couriers_updated_at ON couriers(updated_at);
CREATE INDEX IF NOT EXISTS idx_shipments_updated_at ON shipments(updated_at);
CREATE INDEX IF NOT EXISTS idx_events_updated_at ON shipment_tracking_events(updated_at);
CREATE INDEX IF NOT EXISTS idx_events_shipment_id ON shipment_tracking_events(shipment_id);
CREATE INDEX IF NOT EXISTS idx_cod_updated_at ON cod_settlements(updated_at);
CREATE INDEX IF NOT EXISTS idx_tickets_updated_at ON customer_tickets(updated_at);
```

### File: `scripts/generate_oltp_data.py`

```python
import random
from datetime import datetime, timedelta
from sqlalchemy import text
from config.db_config import get_oltp_conn
from config.settings import (
    DEFAULT_CUSTOMERS,
    DEFAULT_HUBS,
    DEFAULT_COURIERS,
    DEFAULT_VEHICLES,
    DEFAULT_SHIPMENTS,
    SIMULATE_NEW_SHIPMENTS,
)

CITIES = [
    ("Hà Nội", ["Cầu Giấy", "Đống Đa", "Nam Từ Liêm", "Long Biên"]),
    ("TP. Hồ Chí Minh", ["Quận 1", "Quận 7", "Tân Bình", "Thủ Đức"]),
    ("Đà Nẵng", ["Hải Châu", "Thanh Khê", "Sơn Trà"]),
    ("Hải Phòng", ["Ngô Quyền", "Lê Chân", "Hải An"]),
    ("Cần Thơ", ["Ninh Kiều", "Cái Răng"]),
]


def generate_data():
    with get_oltp_conn() as conn:
        existing_cnt = conn.execute(text("SELECT COUNT(*) FROM shipments")).scalar()
        now_ts = conn.execute(text("SELECT LOCALTIMESTAMP")).scalar()

        if existing_cnt == 0:
            print(f"🌱 Khởi tạo dữ liệu OLTP lần đầu ({DEFAULT_SHIPMENTS} vận đơn + danh mục chuẩn)...")
            _seed_initial_data(conn, now_ts)
        else:
            print(f"🔄 Mô phỏng Incremental Batch ({SIMULATE_NEW_SHIPMENTS} vận đơn mới + cập nhật SCD2 Customers/Couriers)...")
            _simulate_incremental_batch(conn, now_ts, existing_cnt)
        conn.commit()


def _seed_initial_data(conn, now_ts: datetime):
    # 1. Service Types
    services = [
        ("STD", "Giao hàng Tiêu chuẩn", 72, False),
        ("FAST", "Giao hàng Nhanh", 36, True),
        ("SAME_DAY", "Giao hàng Hỏa tốc Trong ngày", 12, True),
        ("ECO", "Giao hàng Tiết kiệm", 120, False),
    ]
    for code, name, sla, is_exp in services:
        conn.execute(
            text("""
                INSERT INTO service_types (service_code, service_name, sla_hours, is_express, created_at, updated_at)
                VALUES (:c, :n, :s, :e, :ts, :ts) ON CONFLICT (service_code) DO NOTHING
            """),
            {"c": code, "n": name, "s": sla, "e": is_exp, "ts": now_ts},
        )

    # 2. Payment Methods
    pms = [
        ("CASH_SENDER", "Tiền mặt (Người gửi trả)", "SENDER", False),
        ("CASH_RECEIVER_COD", "Tiền mặt kèm Thu hộ COD", "RECEIVER", True),
        ("WALLET_COD", "Ví điện tử kèm COD", "SENDER", True),
        ("CORP_MONTHLY", "Công nợ Doanh nghiệp cuối tháng", "CORPORATE", False),
    ]
    for code, name, payer, cod in pms:
        conn.execute(
            text("""
                INSERT INTO payment_methods (payment_method_code, payment_method_name, payer_type, has_cod, created_at, updated_at)
                VALUES (:c, :n, :p, :cod, :ts, :ts) ON CONFLICT (payment_method_code) DO NOTHING
            """),
            {"c": code, "n": name, "p": payer, "cod": cod, "ts": now_ts},
        )

    # 3. Status Reasons
    reasons = [
        ("CREATED", "Đã tạo đơn", "RS_CREATED", "Khách hàng tạo vận đơn trên hệ thống", "BOOKING"),
        ("PICKED_UP", "Đã lấy hàng", "RS_PICKED_UP", "Bưu tá đã lấy hàng thành công", "PICKUP"),
        ("IN_TRANSIT", "Đang trung chuyển", "RS_IN_TRANSIT", "Đang luân chuyển giữa các Hub", "TRANSIT"),
        ("DELIVERING", "Đang giao hàng", "RS_DELIVERING", "Bưu tá đang đi phát hàng", "LAST_MILE"),
        ("DELIVERY_FAILED", "Giao thất bại", "RS_FAIL_ABSENT", "Khách nhận vắng mặt / không nghe máy", "EXCEPTION"),
        ("DELIVERED", "Giao thành công", "RS_DELIVERED_OK", "Người nhận đã ký nhận hàng", "COMPLETION"),
        ("RETURNED", "Đã hoàn hàng", "RS_RETURNED", "Hoàn trả về người gửi sau nhiều lần phát lỗi", "RETURN"),
        ("CANCELLED", "Đã hủy", "RS_CANCELLED", "Người gửi yêu cầu hủy trước khi lấy hàng", "CANCELLATION"),
    ]
    for sc, sn, rc, rn, cat in reasons:
        conn.execute(
            text("""
                INSERT INTO status_reasons (status_code, status_name, reason_code, reason_name, event_category, created_at, updated_at)
                VALUES (:sc, :sn, :rc, :rn, :cat, :ts, :ts) ON CONFLICT (reason_code) DO NOTHING
            """),
            {"sc": sc, "sn": sn, "rc": rc, "rn": rn, "cat": cat, "ts": now_ts},
        )

    # 4. Hubs
    for i in range(1, DEFAULT_HUBS + 1):
        city, districts = random.choice(CITIES)
        dist = random.choice(districts)
        level = "REGIONAL_SORTING" if i <= 5 else "LOCAL_STATION"
        cap = 5000 if level == "REGIONAL_SORTING" else 1500
        conn.execute(
            text("""
                INSERT INTO hubs (hub_code, hub_name, hub_level, city, district, capacity, created_at, updated_at)
                VALUES (:code, :name, :lvl, :city, :dist, :cap, :ts, :ts) ON CONFLICT (hub_code) DO NOTHING
            """),
            {
                "code": f"HUB_{i:03d}",
                "name": f"Bưu cục {dist} - {city} #{i}",
                "lvl": level,
                "city": city,
                "dist": dist,
                "cap": cap,
                "ts": now_ts,
            },
        )

    # 5. Vehicles
    vtypes = [("MOTORBIKE", 50.0), ("VAN_1T", 1000.0), ("TRUCK_5T", 5000.0)]
    for i in range(1, DEFAULT_VEHICLES + 1):
        vt, cap = random.choice(vtypes)
        conn.execute(
            text("""
                INSERT INTO vehicles (vehicle_code, vehicle_type, capacity_kg, created_at, updated_at)
                VALUES (:code, :vt, :cap, :ts, :ts) ON CONFLICT (vehicle_code) DO NOTHING
            """),
            {"code": f"VEH_{i:04d}", "vt": vt, "cap": cap, "ts": now_ts},
        )

    # 6. Customers (kèm vài số điện thoại lỗi định dạng để test Rule 6 Warning)
    tiers = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
    ctypes = ["INDIVIDUAL", "MERCHANT", "ENTERPRISE"]
    for i in range(1, DEFAULT_CUSTOMERS + 1):
        city, districts = random.choice(CITIES)
        phone = f"09{random.randint(10000000, 99999999)}" if i % 50 != 0 else "INVALID-PHONE-###"
        conn.execute(
            text("""
                INSERT INTO customers (customer_code, customer_name, customer_type, membership_tier, phone_number, city, district, created_at, updated_at)
                VALUES (:code, :name, :ct, :tier, :ph, :city, :dist, :ts, :ts) ON CONFLICT (customer_code) DO NOTHING
            """),
            {
                "code": f"CUST_{i:05d}",
                "name": f"Khách hàng {i}",
                "ct": random.choice(ctypes),
                "tier": random.choice(tiers),
                "ph": phone,
                "city": city,
                "dist": random.choice(districts),
                "ts": now_ts,
            },
        )

    # 7. Couriers
    for i in range(1, DEFAULT_COURIERS + 1):
        conn.execute(
            text("""
                INSERT INTO couriers (courier_code, courier_name, hub_id, contract_type, vehicle_id, effective_date, created_at, updated_at)
                VALUES (:code, :name, :hub, :ct, :veh, :eff, :ts, :ts) ON CONFLICT (courier_code) DO NOTHING
            """),
            {
                "code": f"CR_{i:04d}",
                "name": f"Bưu tá {i}",
                "hub": random.randint(1, DEFAULT_HUBS),
                "ct": random.choice(["FULL_TIME", "PART_TIME", "PARTNER"]),
                "veh": random.randint(1, DEFAULT_VEHICLES),
                "eff": (now_ts - timedelta(days=90)).date(),
                "ts": now_ts,
            },
        )

    _insert_shipments_and_children(conn, now_ts, start_idx=1, count=DEFAULT_SHIPMENTS)


def _simulate_incremental_batch(conn, now_ts: datetime, existing_cnt: int):
    # Cập nhật SCD2 cho 15 khách hàng và 10 bưu tá
    conn.execute(
        text("""
            UPDATE customers
            SET membership_tier = CASE WHEN membership_tier = 'PLATINUM' THEN 'GOLD' ELSE 'PLATINUM' END,
                updated_at = :ts
            WHERE customer_id <= 15
        """),
        {"ts": now_ts},
    )
    conn.execute(
        text("""
            UPDATE couriers
            SET hub_id = (hub_id % :max_hub) + 1,
                contract_type = CASE WHEN contract_type = 'FULL_TIME' THEN 'PARTNER' ELSE 'FULL_TIME' END,
                effective_date = :eff_date,
                updated_at = :ts
            WHERE courier_id <= 10
        """),
        {"max_hub": DEFAULT_HUBS, "eff_date": now_ts.date(), "ts": now_ts},
    )
    _insert_shipments_and_children(conn, now_ts, start_idx=existing_cnt + 1, count=SIMULATE_NEW_SHIPMENTS)


def _insert_shipments_and_children(conn, now_ts: datetime, start_idx: int, count: int):
    statuses = ["DELIVERED", "DELIVERED", "DELIVERED", "IN_TRANSIT", "RETURNED", "CANCELLED"]
    for idx in range(start_idx, start_idx + count):
        order_dt = now_ts - timedelta(days=random.randint(2, 25), hours=random.randint(1, 12))
        pickup_dt = order_dt + timedelta(hours=random.randint(1, 4))
        promised_dt = order_dt + timedelta(hours=random.choice([24, 36, 72]))
        status = random.choice(statuses)
        actual_deliv_dt = (
            pickup_dt + timedelta(hours=random.randint(6, 60))
            if status in ("DELIVERED", "RETURNED")
            else None
        )

        # Cố tình tạo vài bản ghi lỗi Blocker (weight <= 0) để kiểm thử DATA_QUARANTINE
        weight = -2.5 if idx % 250 == 0 else round(random.uniform(0.5, 25.0), 2)
        origin_hub = random.randint(1, DEFAULT_HUBS)
        dest_hub = random.randint(1, DEFAULT_HUBS)
        cod_amt = round(random.choice([0, 0, 150000, 350000, 500000]), 2)

        ship_id = conn.execute(
            text("""
                INSERT INTO shipments (
                    shipment_number, sender_customer_id, receiver_customer_id,
                    origin_hub_id, destination_hub_id, service_type_id, payment_method_id,
                    weight, distance_km, freight_charge, cod_amount,
                    order_date, pickup_date, promised_delivery_date, actual_delivery_date,
                    current_status, created_at, updated_at
                ) VALUES (
                    :snum, :send_id, :recv_id, :orig, :dest, :stype, :pm,
                    :w, :dist, :fc, :cod, :odt, :pdt, :pddt, :addt, :st, :ts, :ts
                ) RETURNING shipment_id
            """),
            {
                "snum": f"EXP{idx:08d}",
                "send_id": random.randint(1, DEFAULT_CUSTOMERS),
                "recv_id": None if idx % 100 == 0 else random.randint(1, DEFAULT_CUSTOMERS),
                "orig": origin_hub,
                "dest": dest_hub,
                "stype": random.randint(1, 4),
                "pm": 2 if cod_amt > 0 else 1,
                "w": weight,
                "dist": round(random.uniform(5.0, 800.0), 2),
                "fc": round(random.uniform(20000, 120000), 2),
                "cod": cod_amt,
                "odt": order_dt,
                "pdt": pickup_dt,
                "pddt": promised_dt,
                "addt": actual_deliv_dt,
                "st": status,
                "ts": now_ts,
            },
        ).scalar()

        # Sinh chuỗi sự kiện hành trình cho vận đơn
        ev_list = [
            (pickup_dt, "PICKED_UP", origin_hub, 2),
            (pickup_dt + timedelta(hours=1), "INBOUND_SCAN", origin_hub, 3),
            (pickup_dt + timedelta(hours=3), "OUTBOUND_SCAN", origin_hub, 3),
        ]
        if status == "DELIVERED" and actual_deliv_dt:
            ev_list.append((actual_deliv_dt - timedelta(hours=2), "INBOUND_SCAN", dest_hub, 3))
            ev_list.append((actual_deliv_dt, "DELIVERED", dest_hub, 6))
        elif status == "RETURNED" and actual_deliv_dt:
            ev_list.append((actual_deliv_dt - timedelta(hours=3), "DELIVERY_ATTEMPT", dest_hub, 5))
            ev_list.append((actual_deliv_dt, "RETURNED", dest_hub, 7))

        for ev_time, ev_type, hub_id, sr_id in ev_list:
            conn.execute(
                text("""
                    INSERT INTO shipment_tracking_events (
                        shipment_id, event_time, event_type, hub_id, courier_id,
                        vehicle_id, status_reason_id, scan_source, created_at, updated_at
                    ) VALUES (
                        :sid, :et, :etype, :hub, :cr, :veh, :sr, 'PDA_SCANNER', :ts, :ts
                    )
                """),
                {
                    "sid": ship_id,
                    "et": ev_time,
                    "etype": ev_type,
                    "hub": hub_id,
                    "cr": random.randint(1, DEFAULT_COURIERS),
                    "veh": random.randint(1, DEFAULT_VEHICLES),
                    "sr": sr_id,
                    "ts": now_ts,
                },
            )

        if cod_amt > 0 and status == "DELIVERED" and actual_deliv_dt:
            conn.execute(
                text("""
                    INSERT INTO cod_settlements (
                        shipment_id, cod_amount, settlement_status, settled_at, created_at, updated_at
                    ) VALUES (:sid, :cod, 'SETTLED', :sat, :ts, :ts)
                """),
                {"sid": ship_id, "cod": cod_amt, "sat": actual_deliv_dt + timedelta(hours=12), "ts": now_ts},
            )

        if idx % 40 == 0:
            conn.execute(
                text("""
                    INSERT INTO customer_tickets (
                        shipment_id, customer_id, issue_type, ticket_status, created_at, updated_at
                    ) VALUES (:sid, :cid, 'DELAYED_DELIVERY', 'RESOLVED', :ts, :ts)
                """),
                {"sid": ship_id, "cid": random.randint(1, DEFAULT_CUSTOMERS), "ts": now_ts},
            )
```

---

## 4. Nhóm SQL Khởi tạo Snowflake (`sql/snowflake_init/01_init_snowflake.sql`)

```sql
-- ============================================================================
-- Khởi tạo Warehouse, Database, 4 Schemas (STG, SILVER, GOLD, META) trên Snowflake
-- ============================================================================
CREATE WAREHOUSE IF NOT EXISTS EXPRESS_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS EXPRESS_DWH;
USE DATABASE EXPRESS_DWH;

CREATE SCHEMA IF NOT EXISTS STG;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS META;

-- ============================================================================
-- 1. TẦNG META: Quản lý Watermark, Batch Audit Log & Data Quarantine
-- ============================================================================
CREATE TABLE IF NOT EXISTS META.ETL_WATERMARK (
    TABLE_NAME VARCHAR(100) PRIMARY KEY,
    LAST_EXTRACTED_TIMESTAMP TIMESTAMP_NTZ NOT NULL DEFAULT '1970-01-01 00:00:00'::TIMESTAMP_NTZ,
    LAST_BATCH_ID VARCHAR(64),
    UPDATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
);

MERGE INTO META.ETL_WATERMARK tgt
USING (
    SELECT column1 AS TABLE_NAME FROM VALUES
    ('customers'), ('hubs'), ('vehicles'), ('couriers'), ('service_types'),
    ('payment_methods'), ('status_reasons'), ('shipments'),
    ('shipment_tracking_events'), ('cod_settlements'), ('customer_tickets')
) src
ON tgt.TABLE_NAME = src.TABLE_NAME
WHEN NOT MATCHED THEN
    INSERT (TABLE_NAME, LAST_EXTRACTED_TIMESTAMP)
    VALUES (src.TABLE_NAME, '1970-01-01 00:00:00'::TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS META.ETL_BATCH_LOG (
    BATCH_ID VARCHAR(64) NOT NULL,
    TABLE_NAME VARCHAR(100) NOT NULL,
    RUN_START TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    RUN_END TIMESTAMP_NTZ,
    ROWS_EXTRACTED INT DEFAULT 0,
    ROWS_LOADED INT DEFAULT 0,
    ROWS_REJECTED INT DEFAULT 0,
    STATUS VARCHAR(30) NOT NULL, -- RUNNING / EXTRACTED / SUCCESS / FAILED
    ERROR_MESSAGE STRING,
    SOURCE_SNAPSHOT_TS TIMESTAMP_NTZ, -- [AUDIT-A3] Mốc snapshot OLTP của batch
    PRIMARY KEY (BATCH_ID, TABLE_NAME)
);

CREATE TABLE IF NOT EXISTS META.DATA_QUARANTINE (
    QUARANTINE_ID INT IDENTITY(1,1) PRIMARY KEY,
    BATCH_ID VARCHAR(64) NOT NULL,
    SOURCE_TABLE VARCHAR(100) NOT NULL,
    RECORD_ID VARCHAR(100) NOT NULL,
    RULE_NAME VARCHAR(100) NOT NULL,
    SEVERITY VARCHAR(20) NOT NULL, -- Blocker / Warning
    ERROR_MESSAGE STRING NOT NULL,
    PAYLOAD VARIANT,
    CREATED_AT TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    IS_RESOLVED BOOLEAN NOT NULL DEFAULT FALSE
);

-- ============================================================================
-- 2. TẦNG STG (BRONZE): Lưu dữ liệu thô trích xuất từ PostgreSQL OLTP theo batch_id
-- ============================================================================
CREATE TABLE IF NOT EXISTS STG.CUSTOMERS (
    CUSTOMER_ID INT, CUSTOMER_CODE VARCHAR(30), CUSTOMER_NAME VARCHAR(150),
    CUSTOMER_TYPE VARCHAR(30), MEMBERSHIP_TIER VARCHAR(30), PHONE_NUMBER VARCHAR(30),
    CITY VARCHAR(100), DISTRICT VARCHAR(100),
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.HUBS (
    HUB_ID INT, HUB_CODE VARCHAR(30), HUB_NAME VARCHAR(150),
    HUB_LEVEL VARCHAR(50), CITY VARCHAR(100), DISTRICT VARCHAR(100), CAPACITY INT,
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.VEHICLES (
    VEHICLE_ID INT, VEHICLE_CODE VARCHAR(30), VEHICLE_TYPE VARCHAR(50),
    CAPACITY_KG NUMBER(10, 2),
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.COURIERS (
    COURIER_ID INT, COURIER_CODE VARCHAR(30), COURIER_NAME VARCHAR(150),
    HUB_ID INT, CONTRACT_TYPE VARCHAR(30), VEHICLE_ID INT, EFFECTIVE_DATE DATE,
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.SERVICE_TYPES (
    SERVICE_TYPE_ID INT, SERVICE_CODE VARCHAR(30), SERVICE_NAME VARCHAR(100),
    SLA_HOURS INT, IS_EXPRESS BOOLEAN,
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.PAYMENT_METHODS (
    PAYMENT_METHOD_ID INT, PAYMENT_METHOD_CODE VARCHAR(30), PAYMENT_METHOD_NAME VARCHAR(100),
    PAYER_TYPE VARCHAR(50), HAS_COD BOOLEAN,
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.STATUS_REASONS (
    STATUS_REASON_ID INT, STATUS_CODE VARCHAR(30), STATUS_NAME VARCHAR(100),
    REASON_CODE VARCHAR(30), REASON_NAME VARCHAR(150), EVENT_CATEGORY VARCHAR(50),
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.SHIPMENTS (
    SHIPMENT_ID BIGINT, SHIPMENT_NUMBER VARCHAR(40),
    SENDER_CUSTOMER_ID INT, RECEIVER_CUSTOMER_ID INT,
    ORIGIN_HUB_ID INT, DESTINATION_HUB_ID INT,
    SERVICE_TYPE_ID INT, PAYMENT_METHOD_ID INT,
    WEIGHT NUMBER(10, 2), DISTANCE_KM NUMBER(10, 2),
    FREIGHT_CHARGE NUMBER(12, 2), COD_AMOUNT NUMBER(12, 2),
    ORDER_DATE TIMESTAMP_NTZ, PICKUP_DATE TIMESTAMP_NTZ,
    PROMISED_DELIVERY_DATE TIMESTAMP_NTZ, ACTUAL_DELIVERY_DATE TIMESTAMP_NTZ,
    CURRENT_STATUS VARCHAR(30),
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.SHIPMENT_TRACKING_EVENTS (
    EVENT_ID BIGINT, SHIPMENT_ID BIGINT, EVENT_TIME TIMESTAMP_NTZ,
    EVENT_TYPE VARCHAR(50), HUB_ID INT, COURIER_ID INT,
    VEHICLE_ID INT, STATUS_REASON_ID INT, SCAN_SOURCE VARCHAR(30),
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.COD_SETTLEMENTS (
    SETTLEMENT_ID BIGINT, SHIPMENT_ID BIGINT, COD_AMOUNT NUMBER(12, 2),
    SETTLEMENT_STATUS VARCHAR(30), SETTLED_AT TIMESTAMP_NTZ,
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS STG.CUSTOMER_TICKETS (
    TICKET_ID INT, SHIPMENT_ID BIGINT, CUSTOMER_ID INT,
    ISSUE_TYPE VARCHAR(50), TICKET_STATUS VARCHAR(30),
    CREATED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ,
    BATCH_ID VARCHAR(64), EXTRACTED_AT TIMESTAMP_NTZ
);

-- ============================================================================
-- 3. HÀM TIỆN ÍCH TẦNG GOLD: Tra cứu date_key an toàn [AUDIT-D4]
-- ============================================================================
CREATE OR REPLACE FUNCTION GOLD.FN_DATE_KEY(P_TS TIMESTAMP_NTZ)
RETURNS INT
LANGUAGE SQL
IMMUTABLE
AS
$$
    IFF(
        P_TS IS NULL
        OR P_TS::DATE < '2024-01-01'::DATE
        OR P_TS::DATE > GREATEST('2028-12-31'::DATE, DATEADD('year', 2, CURRENT_DATE())),
        -1,
        TO_NUMBER(TO_VARCHAR(P_TS, 'YYYYMMDD'))
    )
$$;
```

---

## 5. Nhóm Script EL & Data Quality Gate (`scripts/`)

### File: `scripts/extract_load_snowflake.py`

Giữ nguyên toàn bộ thiết kế đã kiểm toán ở bản cũ (`[AUDIT-A1]` $\rightarrow$ `[AUDIT-A5]`): đọc toàn bộ 11 bảng OLTP trong cùng một transaction `REPEATABLE READ`, lùi 10 phút (`EXTRACT_LOOKBACK_MINUTES`), chống 2 batch chạy chồng nhau và nạp thẳng vào Snowflake `STG` bằng `write_pandas`.

```python
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import text
from snowflake.connector.pandas_tools import write_pandas
from config.db_config import get_oltp_conn, get_snowflake_conn

TABLES_TO_EXTRACT = [
    "customers", "hubs", "vehicles", "couriers", "service_types",
    "payment_methods", "status_reasons", "shipments",
    "shipment_tracking_events", "cod_settlements", "customer_tickets"
]

EXTRACT_LOOKBACK_MINUTES = 10
STG_RETENTION_BATCHES = 3


def run_extract_load_snowflake(batch_id: str):
    print(f"📥 [Batch: {batch_id}] Bắt đầu Incremental Extract từ Postgres OLTP -> Snowflake STG...")
    extracted_counts = {}

    with get_snowflake_conn(schema="META") as sf_conn, get_oltp_conn() as oltp_conn:
        sf_cur = sf_conn.cursor()

        # [AUDIT-A5] Chống 2 batch chạy chồng nhau
        sf_cur.execute("""
            SELECT BATCH_ID FROM META.ETL_BATCH_LOG
            WHERE BATCH_ID <> %s
              AND STATUS IN ('RUNNING', 'EXTRACTED')
              AND RUN_START > DATEADD('hour', -6, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ)
            ORDER BY RUN_START DESC
            LIMIT 1
        """, (batch_id,))
        running_row = sf_cur.fetchone()
        if running_row:
            raise RuntimeError(
                f"Batch {running_row[0]} đang chạy dở. Chờ batch đó xong hoặc đánh dấu FAILED trong META.ETL_BATCH_LOG."
            )

        # [AUDIT-A1] Đọc cả 11 bảng trong cùng 1 snapshot REPEATABLE READ trên Postgres OLTP
        src_conn = oltp_conn.execution_options(isolation_level="REPEATABLE READ")
        snapshot_ts = src_conn.execute(text("SELECT LOCALTIMESTAMP")).scalar()

        for table in TABLES_TO_EXTRACT:
            sf_cur.execute("""
                MERGE INTO META.ETL_BATCH_LOG tgt
                USING (SELECT %s AS BATCH_ID, %s AS TABLE_NAME) src
                ON tgt.BATCH_ID = src.BATCH_ID AND tgt.TABLE_NAME = src.TABLE_NAME
                WHEN MATCHED THEN UPDATE SET
                    RUN_START = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                    STATUS = 'RUNNING',
                    ERROR_MESSAGE = NULL
                WHEN NOT MATCHED THEN INSERT (BATCH_ID, TABLE_NAME, RUN_START, STATUS)
                    VALUES (src.BATCH_ID, src.TABLE_NAME, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ, 'RUNNING')
            """, (batch_id, table))

            try:
                sf_cur.execute(
                    "SELECT LAST_EXTRACTED_TIMESTAMP FROM META.ETL_WATERMARK WHERE TABLE_NAME = %s",
                    (table,)
                )
                wm_row = sf_cur.fetchone()
                wm_ts = wm_row[0] if wm_row and wm_row[0] else datetime(1970, 1, 1)
                # [AUDIT-A2] Cửa sổ chồng lấn lookback 10 phút chống sót giao dịch commit muộn
                from_ts = wm_ts - timedelta(minutes=EXTRACT_LOOKBACK_MINUTES)

                query = text(f"SELECT * FROM {table} WHERE updated_at > :from_ts").bindparams(from_ts=from_ts)
                df = pd.read_sql(query, src_conn)
                row_count = len(df)
                extracted_counts[table] = row_count

                # [AUDIT-A4] Xóa dữ liệu nếu chạy lại cùng batch_id + chỉ giữ lại N batch SUCCESS gần nhất
                sf_cur.execute(f"""
                    DELETE FROM STG.{table.upper()}
                    WHERE BATCH_ID = %s
                       OR BATCH_ID IN (
                            SELECT BATCH_ID FROM META.ETL_BATCH_LOG
                            WHERE TABLE_NAME = %s AND STATUS = 'SUCCESS'
                            ORDER BY RUN_START DESC
                            OFFSET {STG_RETENTION_BATCHES}
                       )
                """, (batch_id, table))

                if row_count > 0:
                    df["batch_id"] = batch_id
                    df["extracted_at"] = pd.Timestamp(snapshot_ts)
                    df.columns = [c.upper() for c in df.columns]

                    # Chuyển mọi cột datetime (ns/us/ms/tz) sang chuỗi ISO chuẩn và giữ NULL (None) cho NaT
                    for col in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
                        formatted = df[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
                        df[col] = formatted.where(df[col].notna(), None)

                    write_pandas(
                        conn=sf_conn,
                        df=df,
                        table_name=table.upper(),
                        schema="STG",
                        auto_create_table=False,
                        quote_identifiers=False,
                    )

                sf_cur.execute("""
                    UPDATE META.ETL_BATCH_LOG
                    SET ROWS_EXTRACTED = %s,
                        SOURCE_SNAPSHOT_TS = %s,
                        STATUS = 'EXTRACTED'
                    WHERE BATCH_ID = %s AND TABLE_NAME = %s
                """, (row_count, snapshot_ts, batch_id, table))
                print(f"   ✔ {table}: Extracted {row_count} rows (updated_at > {from_ts}, snapshot {snapshot_ts})")

            except Exception as exc:
                sf_cur.execute("""
                    UPDATE META.ETL_BATCH_LOG
                    SET RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                        STATUS = 'FAILED',
                        ERROR_MESSAGE = %s
                    WHERE BATCH_ID = %s AND TABLE_NAME = %s
                """, (str(exc)[:4000], batch_id, table))
                raise

        src_conn.rollback()

    return extracted_counts
```

### File: `scripts/dq_gate_snowflake.py`

Thực thi toàn bộ 11 quy tắc Data Quality Gate (`Blocker` & `Warning`) trên Snowflake `STG` và ghi vào `META.DATA_QUARANTINE` trước khi `dbt run` nạp sang `SILVER` (giữ đúng 100% logic `[AUDIT-B1]` $\rightarrow$ `[AUDIT-B10]` của bản cũ).

```python
from config.db_config import get_snowflake_conn


def _table_exists(cur, schema: str, table_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM EXPRESS_DWH.INFORMATION_SCHEMA.TABLES "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (schema, table_name),
    )
    return cur.fetchone()[0] > 0


VALID_SHIPMENT_STATUSES = (
    "('CREATED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERING', "
    "'DELIVERY_FAILED', 'DELIVERED', 'RETURNED', 'CANCELLED')"
)


def run_dq_gate_snowflake(batch_id: str):
    print(f"🛡️ [Batch: {batch_id}] Chạy Data Quality Gate & Quarantine trên Snowflake...")

    with get_snowflake_conn(schema="META") as sf_conn:
        cur = sf_conn.cursor()

        # Kiểm tra bảng SILVER_SHIPMENTS và SILVER_SHIPMENT_TRACKING_EVENTS (tên bảng do dbt tạo)
        silver_shipments_exists = _table_exists(cur, "SILVER", "SILVER_SHIPMENTS")
        silver_events_exists = _table_exists(cur, "SILVER", "SILVER_SHIPMENT_TRACKING_EVENTS")

        # Idempotent khi chạy lại cùng batch_id
        cur.execute("DELETE FROM META.DATA_QUARANTINE WHERE BATCH_ID = %s", (batch_id,))

        # Rule 1: weight <= 0 (Blocker) - [AUDIT-B1] record_id = SHIPMENT_ID
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_WEIGHT_POSITIVE', 'Blocker',
                'Trọng lượng vận đơn nhỏ hơn hoặc bằng 0 kg',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s AND (s.WEIGHT IS NULL OR s.WEIGHT <= 0)
        """, (batch_id, batch_id))

        # Rule 2: actual_delivery_date < order_date (Blocker)
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_DELIVERY_AFTER_ORDER', 'Blocker',
                'Thời gian giao hàng thực tế nhỏ hơn thời gian đặt hàng',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s
              AND s.ACTUAL_DELIVERY_DATE IS NOT NULL
              AND s.ACTUAL_DELIVERY_DATE < s.ORDER_DATE
        """, (batch_id, batch_id))

        # Rule 3: Thiếu trường bắt buộc / trạng thái không hợp lệ (Blocker) - [AUDIT-B2]
        cur.execute(f"""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_VALID_SHIPMENT_CORE', 'Blocker',
                'Thiếu số vận đơn/khách gửi/ngày đặt/ngày hẹn giao/cước hoặc trạng thái không nằm trong danh mục chuẩn',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s
              AND (
                  s.SHIPMENT_NUMBER IS NULL
                  OR s.SENDER_CUSTOMER_ID IS NULL
                  OR s.ORDER_DATE IS NULL
                  OR s.PROMISED_DELIVERY_DATE IS NULL
                  OR s.FREIGHT_CHARGE IS NULL
                  OR s.CURRENT_STATUS IS NULL
                  OR TRIM(s.CURRENT_STATUS) NOT IN {VALID_SHIPMENT_STATUSES}
              )
        """, (batch_id, batch_id))

        # Rule 3c: Mốc thời gian ở tương lai so với thời điểm trích xuất (Blocker) - [AUDIT-B9]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_NO_FUTURE_TIMESTAMP', 'Blocker',
                'Ngày đặt / lấy hàng / giao hàng nằm sau thời điểm trích xuất dữ liệu nguồn',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s
              AND GREATEST(
                  s.ORDER_DATE,
                  COALESCE(s.PICKUP_DATE, s.ORDER_DATE),
                  COALESCE(s.ACTUAL_DELIVERY_DATE, s.ORDER_DATE)
              ) > DATEADD('minute', 5, s.EXTRACTED_AT)
        """, (batch_id, batch_id))

        # Rule 3d: Ngày đặt hàng muộn hơn sự kiện đã có ở Silver (Blocker) - [AUDIT-B10]
        if silver_shipments_exists and silver_events_exists:
            cur.execute("""
                INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
                SELECT
                    %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                    'RULE_ORDER_AFTER_EVENTS', 'Blocker',
                    'Ngày đặt hàng muộn hơn sự kiện hành trình đã ghi nhận của chính vận đơn',
                    OBJECT_CONSTRUCT(s.*)
                FROM STG.SHIPMENTS s
                WHERE s.BATCH_ID = %s
                  AND EXISTS (
                      SELECT 1 FROM SILVER.SILVER_SHIPMENT_TRACKING_EVENTS e
                      WHERE e.SHIPMENT_ID = s.SHIPMENT_ID AND e.EVENT_TIME < s.ORDER_DATE
                  )
            """, (batch_id, batch_id))

        # Rule 3b: Thiếu khóa ngoại phụ (Warning -> Map Unknown -1 ở Gold) - [AUDIT-B3]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipments', TO_VARCHAR(s.SHIPMENT_ID),
                'RULE_MISSING_SHIPMENT_FK', 'Warning',
                'Thiếu người nhận/hub đi/hub đến/dịch vụ/phương thức thanh toán -> Sẽ map vào dòng Unknown (SK = -1)',
                OBJECT_CONSTRUCT(s.*)
            FROM STG.SHIPMENTS s
            WHERE s.BATCH_ID = %s
              AND (
                  s.RECEIVER_CUSTOMER_ID IS NULL OR s.ORIGIN_HUB_ID IS NULL OR s.DESTINATION_HUB_ID IS NULL
                  OR s.SERVICE_TYPE_ID IS NULL OR s.PAYMENT_METHOD_ID IS NULL
              )
        """, (batch_id, batch_id))

        # Rule 4: event_time < order_date (Blocker - Đối chiếu cả STG lẫn SILVER ở mọi lần chạy)
        if silver_shipments_exists:
            cur.execute("""
                INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
                SELECT
                    %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                    'RULE_EVENT_TIME_VALID', 'Blocker',
                    'Thời gian quét sự kiện nhỏ hơn thời gian tạo vận đơn gốc',
                    OBJECT_CONSTRUCT(e.*)
                FROM STG.SHIPMENT_TRACKING_EVENTS e
                LEFT JOIN STG.SHIPMENTS s_stg
                  ON e.SHIPMENT_ID = s_stg.SHIPMENT_ID AND s_stg.BATCH_ID = %s
                LEFT JOIN SILVER.SILVER_SHIPMENTS s_sil
                  ON e.SHIPMENT_ID = s_sil.SHIPMENT_ID
                WHERE e.BATCH_ID = %s
                  AND e.EVENT_TIME < COALESCE(s_stg.ORDER_DATE, s_sil.ORDER_DATE)
            """, (batch_id, batch_id, batch_id))
        else:
            cur.execute("""
                INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
                SELECT
                    %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                    'RULE_EVENT_TIME_VALID', 'Blocker',
                    'Thời gian quét sự kiện nhỏ hơn thời gian tạo vận đơn gốc',
                    OBJECT_CONSTRUCT(e.*)
                FROM STG.SHIPMENT_TRACKING_EVENTS e
                JOIN STG.SHIPMENTS s_stg
                  ON e.SHIPMENT_ID = s_stg.SHIPMENT_ID AND s_stg.BATCH_ID = %s
                WHERE e.BATCH_ID = %s
                  AND e.EVENT_TIME < s_stg.ORDER_DATE
            """, (batch_id, batch_id, batch_id))

        # Rule 4b: Thiếu trường bắt buộc của sự kiện (Blocker) - [AUDIT-B4]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                'RULE_EVENT_REQUIRED_FIELDS', 'Blocker',
                'Sự kiện thiếu shipment_id, event_time hoặc event_type',
                OBJECT_CONSTRUCT(e.*)
            FROM STG.SHIPMENT_TRACKING_EVENTS e
            WHERE e.BATCH_ID = %s
              AND (e.SHIPMENT_ID IS NULL OR e.EVENT_TIME IS NULL OR e.EVENT_TYPE IS NULL)
        """, (batch_id, batch_id))

        # Rule 4c: Sự kiện quét ở tương lai so với thời điểm trích xuất (Blocker) - [AUDIT-B9]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                'RULE_NO_FUTURE_TIMESTAMP', 'Blocker',
                'Thời gian quét sự kiện nằm sau thời điểm trích xuất dữ liệu nguồn',
                OBJECT_CONSTRUCT(e.*)
            FROM STG.SHIPMENT_TRACKING_EVENTS e
            WHERE e.BATCH_ID = %s
              AND e.EVENT_TIME > DATEADD('minute', 5, e.EXTRACTED_AT)
        """, (batch_id, batch_id))

        # Rule 5: Thiếu courier_id hoặc hub_id trong tracking_events (Warning -> Map Unknown -1)
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'shipment_tracking_events', TO_VARCHAR(e.EVENT_ID),
                'RULE_MISSING_HUB_OR_COURIER', 'Warning',
                'Sự kiện thiếu hub_id hoặc courier_id -> Sẽ map vào dòng Unknown (SK = -1)',
                OBJECT_CONSTRUCT(e.*)
            FROM STG.SHIPMENT_TRACKING_EVENTS e
            WHERE e.BATCH_ID = %s AND (e.HUB_ID IS NULL OR e.COURIER_ID IS NULL)
        """, (batch_id, batch_id))

        # Rule 6: Số điện thoại sai định dạng ở bảng customers (Warning) - [AUDIT-B6]
        cur.execute("""
            INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
            SELECT
                %s, 'customers', c.CUSTOMER_CODE,
                'RULE_INVALID_PHONE_FORMAT', 'Warning',
                'Số điện thoại sai định dạng -> Silver loại ký tự lạ; nếu vẫn không hợp lệ thì gán NULL (không bịa số)',
                OBJECT_CONSTRUCT(c.*)
            FROM STG.CUSTOMERS c
            WHERE c.BATCH_ID = %s AND NOT REGEXP_LIKE(COALESCE(c.PHONE_NUMBER, ''), '^[0-9]{9,12}$')
        """, (batch_id, batch_id))

        # Rule 7: [AUDIT-B7] Bản ghi con thuộc vận đơn chưa hợp lệ ở Silver (Chạy cả ở lần đầu và các lần sau)
        for child_table, child_pk in (
            ("SHIPMENT_TRACKING_EVENTS", "EVENT_ID"),
            ("COD_SETTLEMENTS", "SETTLEMENT_ID"),
            ("CUSTOMER_TICKETS", "TICKET_ID"),
        ):
            silver_check_sql = (
                "AND NOT EXISTS (SELECT 1 FROM SILVER.SILVER_SHIPMENTS ss WHERE ss.SHIPMENT_ID = c.SHIPMENT_ID)"
                if silver_shipments_exists else ""
            )
            cur.execute(f"""
                INSERT INTO META.DATA_QUARANTINE (BATCH_ID, SOURCE_TABLE, RECORD_ID, RULE_NAME, SEVERITY, ERROR_MESSAGE, PAYLOAD)
                SELECT
                    %s, '{child_table.lower()}', TO_VARCHAR(c.{child_pk}),
                    'RULE_ORPHAN_SHIPMENT', 'Warning',
                    'Vận đơn cha chưa có/chưa hợp lệ ở Silver -> Giữ ở Silver, tự lên Gold khi vận đơn cha hợp lệ',
                    OBJECT_CONSTRUCT(c.*)
                FROM STG.{child_table} c
                WHERE c.BATCH_ID = %s
                  AND c.SHIPMENT_ID IS NOT NULL
                  {silver_check_sql}
                  AND NOT EXISTS (
                      SELECT 1 FROM STG.SHIPMENTS s_ok
                      WHERE s_ok.BATCH_ID = %s
                        AND s_ok.SHIPMENT_ID = c.SHIPMENT_ID
                        AND NOT EXISTS (
                            SELECT 1 FROM META.DATA_QUARANTINE q
                            WHERE q.BATCH_ID = %s
                              AND q.SOURCE_TABLE = 'shipments'
                              AND q.SEVERITY = 'Blocker'
                              AND q.RECORD_ID = TO_VARCHAR(s_ok.SHIPMENT_ID)
                        )
                  )
            """, (batch_id, batch_id, batch_id, batch_id))

        # [AUDIT-B8] Cập nhật số dòng bị từ chối (COUNT DISTINCT RECORD_ID) vào META.ETL_BATCH_LOG
        cur.execute("""
            UPDATE META.ETL_BATCH_LOG b
            SET ROWS_REJECTED = (
                SELECT COUNT(DISTINCT q.RECORD_ID)
                FROM META.DATA_QUARANTINE q
                WHERE q.BATCH_ID = b.BATCH_ID
                  AND q.SOURCE_TABLE = b.TABLE_NAME
                  AND q.SEVERITY = 'Blocker'
            )
            WHERE b.BATCH_ID = %s
        """, (batch_id,))

        cur.execute(
            "SELECT COUNT(*) FROM META.DATA_QUARANTINE WHERE BATCH_ID = %s AND SEVERITY = 'Blocker'",
            (batch_id,)
        )
        blocker_cnt = cur.fetchone()[0]
        cur.execute(
            "SELECT COUNT(*) FROM META.DATA_QUARANTINE WHERE BATCH_ID = %s AND SEVERITY = 'Warning'",
            (batch_id,)
        )
        warning_cnt = cur.fetchone()[0]
        print(f"✅ Data Quality Gate hoàn tất: {blocker_cnt} Blocker bị cách ly, {warning_cnt} Warning được ghi nhận.")
```

### File: `scripts/post_load_watermark.py`

Cập nhật Watermark từ `SOURCE_SNAPSHOT_TS` (`[AUDIT-A3]`) sau khi toàn bộ các bước dbt (`silver`, `snapshot`, `gold`, `test`) đã hoàn tất thành công, kèm hàm `mark_batch_failed` (`[AUDIT-G1]`).

```python
from config.db_config import get_snowflake_conn
from scripts.extract_load_snowflake import TABLES_TO_EXTRACT


def update_watermark_on_success(batch_id: str):
    print(f"🔄 [Batch: {batch_id}] Cập nhật Watermark & Hoàn tất Batch Audit Log trên Snowflake...")
    with get_snowflake_conn(schema="META") as sf_conn:
        cur = sf_conn.cursor()
        for table in TABLES_TO_EXTRACT:
            # [AUDIT-A3] Watermark mới = SOURCE_SNAPSHOT_TS của batch, không dùng MAX(updated_at)
            cur.execute("""
                UPDATE META.ETL_WATERMARK w
                SET LAST_EXTRACTED_TIMESTAMP = COALESCE(b.SOURCE_SNAPSHOT_TS, w.LAST_EXTRACTED_TIMESTAMP),
                    LAST_BATCH_ID = %s,
                    UPDATED_AT = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                FROM META.ETL_BATCH_LOG b
                WHERE w.TABLE_NAME = %s
                  AND b.BATCH_ID = %s
                  AND b.TABLE_NAME = w.TABLE_NAME
            """, (batch_id, table, batch_id))

            cur.execute("""
                UPDATE META.ETL_BATCH_LOG
                SET RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                    ROWS_LOADED = GREATEST(ROWS_EXTRACTED - ROWS_REJECTED, 0),
                    STATUS = 'SUCCESS'
                WHERE BATCH_ID = %s AND TABLE_NAME = %s
            """, (batch_id, table))
    print("✅ Đã cập nhật Watermark và đóng Batch thành công!")


def mark_batch_failed(batch_id: str, error_message: str):
    """[AUDIT-G1] Đánh dấu FAILED cho mọi bảng chưa SUCCESS khi một bước giữa chừng bị lỗi."""
    with get_snowflake_conn(schema="META") as sf_conn:
        cur = sf_conn.cursor()
        cur.execute("""
            UPDATE META.ETL_BATCH_LOG
            SET STATUS = 'FAILED',
                RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
                ERROR_MESSAGE = LEFT(%s, 4000)
            WHERE BATCH_ID = %s AND STATUS <> 'SUCCESS'
        """, (error_message, batch_id))
```

---

## 6. Toàn bộ Dự án dbt (`dbt_project/`)

### 6.1 Cấu hình dbt & Macros

#### File: `dbt_project/dbt_project.yml`

```yaml
name: 'express_delivery_dwh'
version: '1.0.0'
config-version: 2

profile: 'express_snowflake'

model-paths: ["models"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]

vars:
  batch_id: "MANUAL_BATCH"

models:
  express_delivery_dwh:
    silver:
      +schema: SILVER
      +materialized: incremental
      +incremental_strategy: merge
    gold:
      dimensions:
        +schema: GOLD
        +materialized: table
      facts:
        +schema: GOLD
        +materialized: incremental
        +incremental_strategy: merge
      analytics:
        +schema: GOLD
```

#### File: `dbt_project/packages.yml`

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

#### File: `dbt_project/profiles.yml`

```yaml
express_snowflake:
  target: prod
  outputs:
    prod:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT', 'your_account_locator') }}"
      user: "{{ env_var('SNOWFLAKE_USER', 'your_username') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD', 'your_password') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'ACCOUNTADMIN') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'EXPRESS_DWH') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'EXPRESS_WH') }}"
      schema: PUBLIC
      threads: 4
      client_session_keep_alive: False
```

#### File: `dbt_project/macros/generate_schema_name.sql`

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim | upper }}
    {%- endif -%}
{%- endmacro %}
```

#### File: `dbt_project/models/sources.yml`

```yaml
version: 2

sources:
  - name: stg
    database: "{{ env_var('SNOWFLAKE_DATABASE', 'EXPRESS_DWH') }}"
    schema: STG
    tables:
      - name: customers
      - name: hubs
      - name: vehicles
      - name: couriers
      - name: service_types
      - name: payment_methods
      - name: status_reasons
      - name: shipments
      - name: shipment_tracking_events
      - name: cod_settlements
      - name: customer_tickets

  - name: meta
    database: "{{ env_var('SNOWFLAKE_DATABASE', 'EXPRESS_DWH') }}"
    schema: META
    tables:
      - name: data_quarantine
```

---

### 6.2 Tầng Silver (`dbt_project/models/silver/`)

> **Chốt chặn bảo vệ dữ liệu (`[AUDIT-B5]`):** Mọi model Silver đều khử trùng lặp bằng `QUALIFY ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY updated_at DESC) = 1` và chỉ nhận các bản ghi không bị đánh dấu `Blocker` trong `META.DATA_QUARANTINE`.

#### File: `dbt_project/models/silver/silver_customers.sql`

```sql
{{ config(unique_key='customer_id') }}

WITH src AS (
    SELECT
        customer_id,
        TRIM(customer_code) AS customer_code,
        TRIM(customer_name) AS customer_name,
        TRIM(customer_type) AS customer_type,
        TRIM(membership_tier) AS membership_tier,
        -- [AUDIT-B6] Không bịa số '0000000000'; chỉ giữ số hợp lệ 9-12 chữ số, còn lại gán NULL
        CASE
            WHEN REGEXP_LIKE(REGEXP_REPLACE(COALESCE(phone_number, ''), '[^0-9]', ''), '^[0-9]{9,12}$')
            THEN REGEXP_REPLACE(phone_number, '[^0-9]', '')
            ELSE NULL
        END AS phone_number,
        TRIM(city) AS city,
        TRIM(district) AS district,
        MD5(
            COALESCE(TRIM(customer_name), '') || '|' ||
            COALESCE(TRIM(customer_type), '') || '|' ||
            COALESCE(TRIM(membership_tier), '') || '|' ||
            COALESCE(TRIM(city), '') || '|' ||
            COALESCE(TRIM(district), '')
        ) AS row_hash,
        updated_at,
        batch_id
    FROM {{ source('stg', 'customers') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.customer_id = tgt.customer_id
WHERE tgt.customer_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_hubs.sql`

```sql
{{ config(unique_key='hub_id') }}

WITH src AS (
    SELECT
        hub_id,
        TRIM(hub_code) AS hub_code,
        TRIM(hub_name) AS hub_name,
        TRIM(hub_level) AS hub_level,
        TRIM(city) AS city,
        TRIM(district) AS district,
        capacity,
        updated_at,
        batch_id
    FROM {{ source('stg', 'hubs') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hub_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.hub_id = tgt.hub_id
WHERE tgt.hub_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_vehicles.sql`

```sql
{{ config(unique_key='vehicle_id') }}

WITH src AS (
    SELECT
        vehicle_id,
        TRIM(vehicle_code) AS vehicle_code,
        TRIM(vehicle_type) AS vehicle_type,
        capacity_kg,
        updated_at,
        batch_id
    FROM {{ source('stg', 'vehicles') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.vehicle_id = tgt.vehicle_id
WHERE tgt.vehicle_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_couriers.sql`

```sql
{{ config(unique_key='courier_id') }}

WITH src AS (
    SELECT
        courier_id,
        TRIM(courier_code) AS courier_code,
        TRIM(courier_name) AS courier_name,
        hub_id,
        TRIM(contract_type) AS contract_type,
        vehicle_id,
        effective_date,
        MD5(
            COALESCE(TRIM(courier_name), '') || '|' ||
            COALESCE(TO_VARCHAR(hub_id), '-1') || '|' ||
            COALESCE(TRIM(contract_type), '') || '|' ||
            COALESCE(TO_VARCHAR(vehicle_id), '-1')
        ) AS row_hash,
        updated_at,
        batch_id
    FROM {{ source('stg', 'couriers') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY courier_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.courier_id = tgt.courier_id
WHERE tgt.courier_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_service_types.sql`

```sql
{{ config(unique_key='service_type_id') }}

WITH src AS (
    SELECT
        service_type_id,
        TRIM(service_code) AS service_code,
        TRIM(service_name) AS service_name,
        sla_hours,
        is_express,
        updated_at,
        batch_id
    FROM {{ source('stg', 'service_types') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service_type_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.service_type_id = tgt.service_type_id
WHERE tgt.service_type_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_payment_methods.sql`

```sql
{{ config(unique_key='payment_method_id') }}

WITH src AS (
    SELECT
        payment_method_id,
        TRIM(payment_method_code) AS payment_method_code,
        TRIM(payment_method_name) AS payment_method_name,
        TRIM(payer_type) AS payer_type,
        has_cod,
        updated_at,
        batch_id
    FROM {{ source('stg', 'payment_methods') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_method_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.payment_method_id = tgt.payment_method_id
WHERE tgt.payment_method_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_status_reasons.sql`

```sql
{{ config(unique_key='status_reason_id') }}

WITH src AS (
    SELECT
        status_reason_id,
        TRIM(status_code) AS status_code,
        TRIM(status_name) AS status_name,
        TRIM(reason_code) AS reason_code,
        TRIM(reason_name) AS reason_name,
        TRIM(event_category) AS event_category,
        updated_at,
        batch_id
    FROM {{ source('stg', 'status_reasons') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status_reason_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.status_reason_id = tgt.status_reason_id
WHERE tgt.status_reason_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_shipments.sql`

```sql
{{ config(unique_key='shipment_id') }}

WITH src AS (
    SELECT
        s.shipment_id,
        TRIM(s.shipment_number) AS shipment_number,
        s.sender_customer_id,
        s.receiver_customer_id, -- [AUDIT-B3] Cho phép NULL, map về SK = -1 ở Gold
        s.origin_hub_id,
        s.destination_hub_id,
        s.service_type_id,
        s.payment_method_id,
        s.weight,
        COALESCE(s.distance_km, 10.0) AS distance_km,
        s.freight_charge,
        COALESCE(s.cod_amount, 0) AS cod_amount,
        s.order_date,
        s.pickup_date,
        s.promised_delivery_date,
        s.actual_delivery_date,
        TRIM(s.current_status) AS current_status,
        s.updated_at,
        s.batch_id
    FROM {{ source('stg', 'shipments') }} s
    WHERE s.batch_id = '{{ var("batch_id") }}'
      AND NOT EXISTS (
          SELECT 1 FROM {{ source('meta', 'data_quarantine') }} q
          WHERE q.batch_id = '{{ var("batch_id") }}'
            AND q.source_table = 'shipments'
            AND q.severity = 'Blocker'
            AND q.record_id = TO_VARCHAR(s.shipment_id)
      )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.shipment_id ORDER BY s.updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.shipment_id = tgt.shipment_id
WHERE tgt.shipment_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_shipment_tracking_events.sql`

```sql
{{ config(unique_key='event_id') }}

-- [AUDIT-B7] Không INNER JOIN silver_shipments để giữ các sự kiện của đơn đến muộn / đang chờ sửa Blocker
WITH src AS (
    SELECT
        e.event_id,
        e.shipment_id,
        e.event_time,
        TRIM(e.event_type) AS event_type,
        e.hub_id,
        e.courier_id,
        e.vehicle_id,
        e.status_reason_id,
        TRIM(e.scan_source) AS scan_source,
        e.updated_at,
        e.batch_id
    FROM {{ source('stg', 'shipment_tracking_events') }} e
    WHERE e.batch_id = '{{ var("batch_id") }}'
      AND NOT EXISTS (
          SELECT 1 FROM {{ source('meta', 'data_quarantine') }} q
          WHERE q.batch_id = '{{ var("batch_id") }}'
            AND q.source_table = 'shipment_tracking_events'
            AND q.severity = 'Blocker'
            AND q.record_id = TO_VARCHAR(e.event_id)
      )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY e.updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.event_id = tgt.event_id
WHERE tgt.event_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_cod_settlements.sql`

```sql
{{ config(unique_key='settlement_id') }}

WITH src AS (
    SELECT
        c.settlement_id,
        c.shipment_id,
        c.cod_amount,
        TRIM(c.settlement_status) AS settlement_status,
        c.settled_at,
        c.updated_at,
        c.batch_id
    FROM {{ source('stg', 'cod_settlements') }} c
    WHERE c.batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.settlement_id ORDER BY c.updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.settlement_id = tgt.settlement_id
WHERE tgt.settlement_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

#### File: `dbt_project/models/silver/silver_customer_tickets.sql`

```sql
{{ config(unique_key='ticket_id') }}

WITH src AS (
    SELECT
        t.ticket_id,
        t.shipment_id,
        t.customer_id,
        TRIM(t.issue_type) AS issue_type,
        TRIM(t.ticket_status) AS ticket_status,
        t.created_at,
        t.updated_at,
        t.batch_id
    FROM {{ source('stg', 'customer_tickets') }} t
    WHERE t.batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.ticket_id ORDER BY t.updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.ticket_id = tgt.ticket_id
WHERE tgt.ticket_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}
```

---

### 6.3 Tầng Snapshots SCD Type 2 (`dbt_project/snapshots/`)

> Sửa lỗi từ bản phác thảo: Snapshot đọc từ `ref('silver_customers')` và `ref('silver_couriers')` (đã qua làm sạch, khử trùng lặp và tính sẵn `row_hash`), không đọc trực tiếp từ `stg`.

#### File: `dbt_project/snapshots/snap_customers.sql`

```sql
{% snapshot snap_customers %}

{{
    config(
      target_schema='SILVER',
      unique_key='customer_id',
      strategy='check',
      check_cols=['row_hash'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    customer_code,
    customer_name,
    customer_type,
    membership_tier,
    phone_number,
    city,
    district,
    row_hash,
    updated_at
FROM {{ ref('silver_customers') }}

{% endsnapshot %}
```

#### File: `dbt_project/snapshots/snap_couriers.sql`

```sql
{% snapshot snap_couriers %}

{{
    config(
      target_schema='SILVER',
      unique_key='courier_id',
      strategy='check',
      check_cols=['row_hash'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    courier_id,
    courier_code,
    courier_name,
    hub_id,
    contract_type,
    vehicle_id,
    effective_date,
    row_hash,
    updated_at
FROM {{ ref('silver_couriers') }}

{% endsnapshot %}
```

---

### 6.4 Tầng Gold — 8 Dimensions (`dbt_project/models/gold/dimensions/`)

> **Bảo toàn chuẩn Kimball:** Cả 8 bảng Dimension đều gắn dòng `Unknown (SK = -1)`. Đặc biệt ở `dim_customer` và `dim_courier`, các khoảng thời gian hiệu lực `[valid_from, valid_to)` được tính lại từ lịch sử các phiên bản snapshot sao cho:
>
> 1. Phiên bản đầu tiên của mỗi NK luôn bắt đầu từ `'1970-01-01 00:00:00'` (tránh trượt Point-in-Time Join cho các đơn hàng lịch sử).
> 2. Mốc chuyển tiếp phiên bản của `dim_courier` ưu tiên `effective_date` (`[AUDIT-C2]`) và đóng/mở liền kề không tạo khe hở.
> 3. Các thuộc tính denormalize của Hub & Vehicle trên `dim_courier` được lấy trực tiếp từ `dim_hub` và `dim_vehicle` hiện hành (`[AUDIT-C1]`).

#### File: `dbt_project/models/gold/dimensions/dim_date.sql`

```sql
WITH date_spine AS (
    SELECT DATEADD('day', SEQ4(), '2024-01-01'::DATE) AS full_date
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
)
SELECT
    -1 AS date_key,
    '1900-01-01'::DATE AS full_date,
    1900 AS year, 1 AS quarter, 1 AS month, 'Unknown' AS month_name,
    1 AS week, 1 AS day_of_week, 1 AS day_of_month,
    FALSE AS is_weekend, FALSE AS is_peak_season
UNION ALL
SELECT
    TO_NUMBER(TO_VARCHAR(full_date, 'YYYYMMDD')) AS date_key,
    full_date,
    YEAR(full_date) AS year,
    QUARTER(full_date) AS quarter,
    MONTH(full_date) AS month,
    TO_VARCHAR(full_date, 'MMMM') AS month_name,
    WEEKISO(full_date) AS week,
    DAYOFWEEKISO(full_date) AS day_of_week,
    DAY(full_date) AS day_of_month,
    IFF(DAYOFWEEKISO(full_date) IN (6, 7), TRUE, FALSE) AS is_weekend,
    IFF(MONTH(full_date) IN (1, 11, 12), TRUE, FALSE) AS is_peak_season
FROM date_spine
WHERE full_date <= GREATEST('2028-12-31'::DATE, DATEADD('year', 2, CURRENT_DATE()))
```

#### File: `dbt_project/models/gold/dimensions/dim_hub.sql`

```sql
SELECT
    -1 AS hub_sk,
    'UNKNOWN' AS hub_nk,
    'Unknown Hub' AS hub_name,
    'Unknown' AS hub_level,
    'Unknown' AS city,
    'Unknown' AS district,
    0 AS capacity,
    TRUE AS is_current
UNION ALL
SELECT
    hub_id AS hub_sk,
    hub_code AS hub_nk,
    hub_name,
    hub_level,
    city,
    district,
    capacity,
    TRUE AS is_current
FROM {{ ref('silver_hubs') }}
```

#### File: `dbt_project/models/gold/dimensions/dim_vehicle.sql`

```sql
SELECT
    -1 AS vehicle_sk,
    'UNKNOWN' AS vehicle_nk,
    'Unknown Vehicle' AS vehicle_type,
    0::NUMBER(10, 2) AS capacity_kg
UNION ALL
SELECT
    vehicle_id AS vehicle_sk,
    vehicle_code AS vehicle_nk,
    vehicle_type,
    capacity_kg
FROM {{ ref('silver_vehicles') }}
```

#### File: `dbt_project/models/gold/dimensions/dim_service_type.sql`

```sql
SELECT
    -1 AS service_type_sk,
    'UNKNOWN' AS service_type_nk,
    'Unknown Service' AS service_name,
    0 AS sla_hours,
    FALSE AS is_express
UNION ALL
SELECT
    service_type_id AS service_type_sk,
    service_code AS service_type_nk,
    service_name,
    sla_hours,
    is_express
FROM {{ ref('silver_service_types') }}
```

#### File: `dbt_project/models/gold/dimensions/dim_status_reason.sql`

```sql
SELECT
    -1 AS status_reason_sk,
    'UNKNOWN' AS status_code,
    'Unknown Status' AS status_name,
    'UNKNOWN' AS reason_code,
    'Unknown Reason' AS reason_name,
    'Unknown' AS event_category
UNION ALL
SELECT
    status_reason_id AS status_reason_sk,
    status_code,
    status_name,
    reason_code,
    reason_name,
    event_category
FROM {{ ref('silver_status_reasons') }}
```

#### File: `dbt_project/models/gold/dimensions/dim_payment_method.sql`

```sql
SELECT
    -1 AS payment_method_sk,
    'UNKNOWN' AS payment_method_nk,
    'Unknown Payment Method' AS payment_method_name,
    'Unknown' AS payer_type,
    FALSE AS has_cod
UNION ALL
SELECT
    payment_method_id AS payment_method_sk,
    payment_method_code AS payment_method_nk,
    payment_method_name,
    payer_type,
    has_cod
FROM {{ ref('silver_payment_methods') }}
```

#### File: `dbt_project/models/gold/dimensions/dim_customer.sql`

```sql
WITH ordered_versions AS (
    SELECT
        customer_id,
        customer_code AS customer_nk,
        customer_name,
        customer_type,
        membership_tier,
        city,
        district,
        row_hash,
        updated_at,
        dbt_valid_from,
        dbt_valid_to,
        ROW_NUMBER() OVER (PARTITION BY customer_code ORDER BY dbt_valid_from, updated_at) AS ver_num,
        LEAD(updated_at) OVER (PARTITION BY customer_code ORDER BY dbt_valid_from, updated_at) AS next_updated_at
    FROM {{ ref('snap_customers') }}
),
resolved_bounds AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY dbt_valid_from, customer_id, ver_num) AS customer_sk,
        customer_nk,
        customer_name,
        customer_type,
        membership_tier,
        city,
        district,
        IFF(ver_num = 1, '1970-01-01 00:00:00'::TIMESTAMP_NTZ, updated_at) AS valid_from,
        COALESCE(next_updated_at, '9999-12-31 23:59:59'::TIMESTAMP_NTZ) AS valid_to,
        IFF(next_updated_at IS NULL AND dbt_valid_to IS NULL, TRUE, FALSE) AS is_current,
        row_hash
    FROM ordered_versions
)
SELECT
    -1 AS customer_sk,
    'UNKNOWN' AS customer_nk,
    'Unknown Customer' AS customer_name,
    'Unknown' AS customer_type,
    'Unknown' AS membership_tier,
    'Unknown' AS city,
    'Unknown' AS district,
    '1900-01-01 00:00:00'::TIMESTAMP_NTZ AS valid_from,
    '9999-12-31 23:59:59'::TIMESTAMP_NTZ AS valid_to,
    TRUE AS is_current,
    'UNKNOWN' AS row_hash
UNION ALL
SELECT * FROM resolved_bounds
```

#### File: `dbt_project/models/gold/dimensions/dim_courier.sql`

```sql
WITH ordered_versions AS (
    SELECT
        c.courier_id,
        c.courier_code AS courier_nk,
        c.courier_name,
        c.hub_id,
        c.contract_type,
        c.vehicle_id,
        c.effective_date,
        c.row_hash,
        c.updated_at,
        c.dbt_valid_from,
        c.dbt_valid_to,
        ROW_NUMBER() OVER (PARTITION BY c.courier_code ORDER BY c.dbt_valid_from, c.updated_at) AS ver_num,
        LAG(c.updated_at) OVER (PARTITION BY c.courier_code ORDER BY c.dbt_valid_from, c.updated_at) AS prev_updated_at
    FROM {{ ref('snap_couriers') }} c
),
calc_bounds AS (
    SELECT
        o.*,
        -- [AUDIT-C2] Chỉ ưu tiên effective_date cho ver >= 2 khi lớn hơn mốc cập nhật của phiên bản liền trước
        IFF(
            ver_num = 1,
            '1970-01-01 00:00:00'::TIMESTAMP_NTZ,
            IFF(
                effective_date IS NOT NULL
                AND effective_date::TIMESTAMP_NTZ > prev_updated_at
                AND effective_date::TIMESTAMP_NTZ <= updated_at,
                effective_date::TIMESTAMP_NTZ,
                updated_at
            )
        ) AS calc_valid_from
    FROM ordered_versions o
),
with_next_bound AS (
    SELECT
        cb.*,
        LEAD(calc_valid_from) OVER (PARTITION BY courier_nk ORDER BY ver_num) AS next_valid_from
    FROM calc_bounds cb
)
SELECT
    -1 AS courier_sk,
    'UNKNOWN' AS courier_nk,
    'Unknown Courier' AS courier_name,
    -1 AS hub_sk,
    'UNKNOWN' AS assigned_hub_code,
    'Unknown Hub' AS assigned_hub_name,
    'Unknown' AS assigned_hub_city,
    'Unknown' AS contract_type,
    -1 AS vehicle_sk,
    'Unknown Vehicle' AS assigned_vehicle_type,
    '1900-01-01 00:00:00'::TIMESTAMP_NTZ AS valid_from,
    '9999-12-31 23:59:59'::TIMESTAMP_NTZ AS valid_to,
    TRUE AS is_current,
    'UNKNOWN' AS row_hash
UNION ALL
SELECT
    ROW_NUMBER() OVER (ORDER BY w.dbt_valid_from, w.courier_id, w.ver_num) AS courier_sk,
    w.courier_nk,
    w.courier_name,
    -- [AUDIT-C1] Đồng bộ thuộc tính SCD1 từ dim_hub & dim_vehicle hiện hành
    COALESCE(dh.hub_sk, -1) AS hub_sk,
    COALESCE(dh.hub_nk, 'UNKNOWN') AS assigned_hub_code,
    COALESCE(dh.hub_name, 'Unknown Hub') AS assigned_hub_name,
    COALESCE(dh.city, 'Unknown') AS assigned_hub_city,
    w.contract_type,
    COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
    COALESCE(dv.vehicle_type, 'Unknown Vehicle') AS assigned_vehicle_type,
    w.calc_valid_from AS valid_from,
    COALESCE(w.next_valid_from, '9999-12-31 23:59:59'::TIMESTAMP_NTZ) AS valid_to,
    IFF(w.next_valid_from IS NULL AND w.dbt_valid_to IS NULL, TRUE, FALSE) AS is_current,
    w.row_hash
FROM with_next_bound w
LEFT JOIN {{ ref('dim_hub') }} dh ON w.hub_id = dh.hub_sk
LEFT JOIN {{ ref('dim_vehicle') }} dv ON w.vehicle_id = dv.vehicle_sk
```

---

### 6.5 Tầng Gold — 3 Facts (`dbt_project/models/gold/facts/`)

#### File: `dbt_project/models/gold/facts/fact_shipment_lifecycle.sql`

```sql
{{
    config(
        materialized='incremental',
        unique_key='shipment_id_nk',
        incremental_strategy='merge',
        cluster_by=['order_date_key', 'origin_hub_sk']
    )
}}

-- [AUDIT-D3, D8] Tập vận đơn cần tính lại: có thay đổi ở bất kỳ bảng nào trong 4 bảng Silver HOẶC cần re-key SCD2
WITH impacted_shipments AS (
    SELECT shipment_id FROM {{ ref('silver_shipments') }}
    {% if is_incremental() %}
    WHERE batch_id = '{{ var("batch_id") }}'
    UNION
    SELECT DISTINCT shipment_id FROM {{ ref('silver_shipment_tracking_events') }} WHERE batch_id = '{{ var("batch_id") }}'
    UNION
    SELECT DISTINCT shipment_id FROM {{ ref('silver_cod_settlements') }} WHERE batch_id = '{{ var("batch_id") }}'
    UNION
    SELECT DISTINCT shipment_id FROM {{ ref('silver_customer_tickets') }} WHERE batch_id = '{{ var("batch_id") }}' AND shipment_id IS NOT NULL
    UNION
    -- [AUDIT-D8] Các vận đơn đã nạp nhưng sender/receiver_customer_sk đang trỏ vào phiên bản SCD2 đã đóng và không còn khớp khoảng hiệu lực
    SELECT f.shipment_id_nk AS shipment_id
    FROM {{ this }} f
    JOIN {{ ref('dim_customer') }} c ON c.customer_sk IN (f.sender_customer_sk, f.receiver_customer_sk)
    WHERE c.is_current = FALSE
      AND NOT (f.order_timestamp >= c.valid_from AND f.order_timestamp < c.valid_to)
    {% endif %}
),
event_milestones AS (
    SELECT
        e.shipment_id,
        MIN(CASE WHEN e.event_type = 'INBOUND_SCAN' THEN e.event_time END) AS first_sorting_time,
        MAX(CASE WHEN e.event_type = 'OUTBOUND_SCAN' THEN e.event_time END) AS last_sorting_time,
        -- [AUDIT-D10] Chỉ đếm DELIVERY_ATTEMPT và DELIVERED là lần phát
        MIN(CASE WHEN e.event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED') THEN e.event_time END) AS first_attempt_time,
        MAX(CASE WHEN e.event_type = 'RETURNED' THEN e.event_time END) AS return_time,
        COUNT(CASE WHEN e.event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED') THEN 1 END) AS delivery_attempt_count
    FROM {{ ref('silver_shipment_tracking_events') }} e
    JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
    GROUP BY e.shipment_id
),
latest_event_reason AS (
    SELECT
        e.shipment_id,
        dsr.status_reason_sk,
        dsr.status_code
    FROM {{ ref('silver_shipment_tracking_events') }} e
    JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
    LEFT JOIN {{ ref('dim_status_reason') }} dsr ON e.status_reason_id = dsr.status_reason_sk
    QUALIFY ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_time DESC, e.event_id DESC) = 1
),
fallback_status_lookup AS (
    SELECT status_code, status_reason_sk
    FROM {{ ref('dim_status_reason') }}
    WHERE status_reason_sk <> -1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status_code ORDER BY status_reason_sk) = 1
),
cod_agg AS (
    SELECT
        c.shipment_id,
        c.settlement_status,
        c.settled_at
    FROM {{ ref('silver_cod_settlements') }} c
    JOIN impacted_shipments imp ON c.shipment_id = imp.shipment_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.shipment_id ORDER BY c.updated_at DESC) = 1
),
ticket_agg AS (
    SELECT
        t.shipment_id,
        COUNT(t.ticket_id) AS ticket_count
    FROM {{ ref('silver_customer_tickets') }} t
    JOIN impacted_shipments imp ON t.shipment_id = imp.shipment_id
    GROUP BY t.shipment_id
)
SELECT
    s.shipment_id AS shipment_sk,
    s.shipment_id AS shipment_id_nk,
    s.shipment_number,
    COALESCE(dc_send.customer_sk, -1) AS sender_customer_sk,
    COALESCE(dc_recv.customer_sk, -1) AS receiver_customer_sk,
    COALESCE(dh_orig.hub_sk, -1) AS origin_hub_sk,
    COALESCE(dh_dest.hub_sk, -1) AS destination_hub_sk,
    COALESCE(dst.service_type_sk, -1) AS service_type_sk,
    COALESCE(dpm.payment_method_sk, -1) AS payment_method_sk,
    -- [AUDIT-D1] Chỉ lấy lý do của sự kiện cuối khi status_code khớp với trạng thái hiện tại của vận đơn
    COALESCE(
        CASE WHEN ler.status_code = s.current_status THEN ler.status_reason_sk END,
        fsl_st.status_reason_sk,
        -1
    ) AS current_status_sk,

    -- Role-playing Date Keys qua hàm an toàn GOLD.FN_DATE_KEY [AUDIT-D4]
    GOLD.FN_DATE_KEY(s.order_date) AS order_date_key,
    GOLD.FN_DATE_KEY(s.pickup_date) AS pickup_date_key,
    GOLD.FN_DATE_KEY(m.first_sorting_time) AS first_sorting_date_key,
    GOLD.FN_DATE_KEY(m.last_sorting_time) AS last_sorting_date_key,
    GOLD.FN_DATE_KEY(m.first_attempt_time) AS first_delivery_attempt_date_key,
    -- [AUDIT-D2] Chỉ đơn DELIVERED mới có final_delivery_date_key
    IFF(s.current_status = 'DELIVERED', GOLD.FN_DATE_KEY(s.actual_delivery_date), -1) AS final_delivery_date_key,
    IFF(s.current_status = 'RETURNED', GOLD.FN_DATE_KEY(COALESCE(m.return_time, s.actual_delivery_date)), -1) AS return_date_key,
    GOLD.FN_DATE_KEY(s.promised_delivery_date) AS promised_delivery_date_key,
    IFF(ca.settlement_status = 'SETTLED', GOLD.FN_DATE_KEY(ca.settled_at), -1) AS cod_settled_date_key,

    -- Milestone Timestamps
    s.order_date AS order_timestamp,
    s.pickup_date AS pickup_timestamp,
    m.first_sorting_time AS first_sorting_timestamp,
    m.last_sorting_time AS last_sorting_timestamp,
    m.first_attempt_time AS first_delivery_attempt_timestamp,
    IFF(s.current_status = 'DELIVERED', s.actual_delivery_date, NULL) AS final_delivery_timestamp,
    s.promised_delivery_date AS promised_delivery_timestamp,
    IFF(ca.settlement_status = 'SETTLED', ca.settled_at, NULL) AS cod_settled_timestamp,

    -- Additive Measures
    s.weight,
    s.freight_charge,
    s.cod_amount,
    s.distance_km,
    COALESCE(m.delivery_attempt_count, 0) AS delivery_attempt_count,
    COALESCE(ta.ticket_count, 0) AS ticket_count,

    -- Lead-time Measures (Hours) - Dùng DATEDIFF chuẩn Snowflake
    ROUND(DATEDIFF('second', s.pickup_date, m.first_sorting_time) / 3600.0, 2) AS pickup_to_sorting_hours,
    IFF(
        s.current_status = 'DELIVERED',
        ROUND(DATEDIFF('second', m.first_sorting_time, s.actual_delivery_date) / 3600.0, 2),
        NULL
    ) AS sorting_to_delivery_hours,
    IFF(
        s.current_status = 'DELIVERED',
        ROUND(DATEDIFF('second', s.order_date, s.actual_delivery_date) / 3600.0, 2),
        NULL
    ) AS total_lead_time_hours,

    -- Operational & SLA Flags
    (s.current_status = 'DELIVERED') AS is_delivered,
    (s.current_status = 'RETURNED') AS is_returned,
    (s.current_status = 'CANCELLED') AS is_cancelled,
    IFF(s.current_status = 'DELIVERED' AND s.actual_delivery_date <= s.promised_delivery_date, TRUE, FALSE) AS is_on_time,
    IFF(s.current_status = 'DELIVERED' AND s.actual_delivery_date > s.promised_delivery_date, TRUE, FALSE) AS is_sla_breached,
    (COALESCE(ca.settlement_status, 'NONE') = 'SETTLED') AS is_cod_settled,
    (COALESCE(ta.ticket_count, 0) > 0) AS has_complaint_ticket,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS created_at,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS updated_at,
    '{{ var("batch_id") }}' AS batch_id
FROM {{ ref('silver_shipments') }} s
JOIN impacted_shipments imp ON s.shipment_id = imp.shipment_id
LEFT JOIN event_milestones m ON s.shipment_id = m.shipment_id
LEFT JOIN latest_event_reason ler ON s.shipment_id = ler.shipment_id
LEFT JOIN fallback_status_lookup fsl_st ON s.current_status = fsl_st.status_code
LEFT JOIN cod_agg ca ON s.shipment_id = ca.shipment_id
LEFT JOIN ticket_agg ta ON s.shipment_id = ta.shipment_id
LEFT JOIN {{ ref('silver_customers') }} sc_send ON s.sender_customer_id = sc_send.customer_id
LEFT JOIN {{ ref('dim_customer') }} dc_send
  ON dc_send.customer_nk = sc_send.customer_code
 AND s.order_date >= dc_send.valid_from
 AND s.order_date < dc_send.valid_to
LEFT JOIN {{ ref('silver_customers') }} sc_recv ON s.receiver_customer_id = sc_recv.customer_id
LEFT JOIN {{ ref('dim_customer') }} dc_recv
  ON dc_recv.customer_nk = sc_recv.customer_code
 AND s.order_date >= dc_recv.valid_from
 AND s.order_date < dc_recv.valid_to
LEFT JOIN {{ ref('dim_hub') }} dh_orig ON s.origin_hub_id = dh_orig.hub_sk
LEFT JOIN {{ ref('dim_hub') }} dh_dest ON s.destination_hub_id = dh_dest.hub_sk
LEFT JOIN {{ ref('dim_service_type') }} dst ON s.service_type_id = dst.service_type_sk
LEFT JOIN {{ ref('dim_payment_method') }} dpm ON s.payment_method_id = dpm.payment_method_sk
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY s.shipment_id
    ORDER BY dc_send.valid_from DESC NULLS LAST, dc_recv.valid_from DESC NULLS LAST
) = 1
```

#### File: `dbt_project/models/gold/facts/fact_tracking_events.sql`

```sql
{{
    config(
        materialized='incremental',
        unique_key='event_id_nk',
        incremental_strategy='merge',
        cluster_by=['event_date_key', 'hub_sk']
    )
}}

-- [AUDIT-D5, D8] Nạp lại toàn bộ sự kiện của các vận đơn bị ảnh hưởng để tính lại event_sequence chính xác
WITH impacted_shipments AS (
    SELECT shipment_id FROM {{ ref('silver_shipments') }}
    {% if is_incremental() %}
    WHERE batch_id = '{{ var("batch_id") }}'
    UNION
    SELECT DISTINCT shipment_id FROM {{ ref('silver_shipment_tracking_events') }} WHERE batch_id = '{{ var("batch_id") }}'
    UNION
    SELECT DISTINCT ss.shipment_id
    FROM {{ this }} f
    JOIN {{ ref('silver_shipments') }} ss ON f.shipment_number = ss.shipment_number
    JOIN {{ ref('dim_courier') }} c ON f.courier_sk = c.courier_sk
    WHERE c.is_current = FALSE
      AND NOT (f.event_timestamp >= c.valid_from AND f.event_timestamp < c.valid_to)
    {% endif %}
),
seq_events AS (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY e.shipment_id ORDER BY e.event_time, e.event_id) AS event_seq
    FROM {{ ref('silver_shipment_tracking_events') }} e
    JOIN impacted_shipments imp ON e.shipment_id = imp.shipment_id
)
SELECT
    e.event_id AS tracking_event_sk,
    e.event_id AS event_id_nk,
    COALESCE(fsl.shipment_sk, -1) AS shipment_sk,
    ss.shipment_number,
    GOLD.FN_DATE_KEY(e.event_time) AS event_date_key,
    e.event_time AS event_timestamp,
    e.event_type,
    COALESCE(dh.hub_sk, -1) AS hub_sk,
    COALESCE(dco.courier_sk, -1) AS courier_sk,
    COALESCE(dv.vehicle_sk, -1) AS vehicle_sk,
    COALESCE(dsr.status_reason_sk, -1) AS status_reason_sk,
    e.event_seq AS event_sequence,
    e.scan_source,
    '{{ var("batch_id") }}' AS batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS loaded_at
FROM seq_events e
JOIN {{ ref('silver_shipments') }} ss ON e.shipment_id = ss.shipment_id
LEFT JOIN {{ ref('fact_shipment_lifecycle') }} fsl ON ss.shipment_id = fsl.shipment_id_nk
LEFT JOIN {{ ref('dim_hub') }} dh ON e.hub_id = dh.hub_sk
LEFT JOIN {{ ref('silver_couriers') }} sco ON e.courier_id = sco.courier_id
LEFT JOIN {{ ref('dim_courier') }} dco
  ON dco.courier_nk = sco.courier_code
 AND e.event_time >= dco.valid_from
 AND e.event_time < dco.valid_to
LEFT JOIN {{ ref('dim_vehicle') }} dv ON e.vehicle_id = dv.vehicle_sk
LEFT JOIN {{ ref('dim_status_reason') }} dsr ON e.status_reason_id = dsr.status_reason_sk
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY e.event_id
    ORDER BY dco.valid_from DESC NULLS LAST
) = 1
```

#### File: `dbt_project/models/gold/facts/fact_daily_hub_snapshot.sql`

```sql
{{
    config(
        materialized='incremental',
        unique_key=['snapshot_date_key', 'hub_sk'],
        incremental_strategy='merge',
        cluster_by=['snapshot_date_key', 'hub_sk']
    )
}}

-- [AUDIT-D6, D7] Lưới ngày liên tục từ ngày sớm nhất có sự kiện thay đổi trong batch tới ngày hoạt động cuối
WITH affected_window AS (
    SELECT
        {% if is_incremental() %}
        COALESCE(
            (SELECT MIN(event_date_key) FROM {{ ref('fact_tracking_events') }} WHERE batch_id = '{{ var("batch_id") }}' AND event_date_key <> -1),
            (SELECT MAX(event_date_key) FROM {{ ref('fact_tracking_events') }} WHERE event_date_key <> -1)
        ) AS start_key,
        {% else %}
        (SELECT MIN(event_date_key) FROM {{ ref('fact_tracking_events') }} WHERE event_date_key <> -1) AS start_key,
        {% endif %}
        (SELECT MAX(event_date_key) FROM {{ ref('fact_tracking_events') }} WHERE event_date_key <> -1) AS end_key
),
snapshot_dates AS (
    SELECT d.date_key AS snapshot_date_key, d.full_date AS snapshot_date
    FROM affected_window w
    JOIN {{ ref('dim_date') }} d ON d.date_key BETWEEN w.start_key AND w.end_key
    WHERE d.date_key <> -1
),
date_hub_grid AS (
    SELECT sd.snapshot_date_key, sd.snapshot_date, h.hub_sk
    FROM snapshot_dates sd
    CROSS JOIN {{ ref('dim_hub') }} h
    WHERE h.hub_sk <> -1 AND h.is_current = TRUE
),
hub_daily_flow AS (
    SELECT
        e.event_date_key AS snapshot_date_key,
        e.hub_sk,
        COUNT_IF(e.event_type = 'INBOUND_SCAN') AS inbound_count,
        COUNT_IF(e.event_type = 'OUTBOUND_SCAN') AS outbound_count,
        COUNT_IF(e.event_type = 'RETURNED') AS return_count
    FROM {{ ref('fact_tracking_events') }} e
    JOIN snapshot_dates sd ON e.event_date_key = sd.snapshot_date_key
    WHERE e.hub_sk <> -1
    GROUP BY e.event_date_key, e.hub_sk
),
hub_backlog_shipments AS (
    SELECT DISTINCT
        g.snapshot_date_key,
        g.snapshot_date,
        g.hub_sk,
        in_ev.shipment_number,
        fsl.destination_hub_sk,
        fsl.cod_amount,
        fsl.cod_settled_timestamp
    FROM date_hub_grid g
    JOIN {{ ref('fact_tracking_events') }} in_ev
      ON in_ev.hub_sk = g.hub_sk
     AND in_ev.event_date_key <= g.snapshot_date_key
     AND in_ev.event_type = 'INBOUND_SCAN'
    JOIN {{ ref('fact_shipment_lifecycle') }} fsl
      ON in_ev.shipment_number = fsl.shipment_number
    WHERE NOT EXISTS (
        SELECT 1 FROM {{ ref('fact_tracking_events') }} out_ev
        WHERE out_ev.shipment_number = in_ev.shipment_number
          AND out_ev.event_date_key <= g.snapshot_date_key
          AND out_ev.event_timestamp >= in_ev.event_timestamp
          AND (
              (out_ev.hub_sk = in_ev.hub_sk AND out_ev.event_type = 'OUTBOUND_SCAN')
              OR out_ev.event_type IN ('DELIVERED', 'RETURNED')
              OR (out_ev.hub_sk <> -1 AND out_ev.hub_sk <> in_ev.hub_sk AND out_ev.event_type = 'INBOUND_SCAN')
          )
    )
),
hub_backlog_agg AS (
    SELECT
        b.snapshot_date_key,
        b.hub_sk,
        COUNT(b.shipment_number) AS backlog_count,
        -- [AUDIT-D7] Đơn đang tồn kho tại chính Hub đích cuối ngày = đơn chờ giao chặng cuối
        COUNT_IF(b.hub_sk = b.destination_hub_sk) AS pending_delivery_count,
        COALESCE(SUM(
            IFF(
                b.cod_amount > 0 AND (b.cod_settled_timestamp IS NULL OR b.cod_settled_timestamp::DATE > b.snapshot_date),
                b.cod_amount,
                0
            )
        ), 0) AS cod_held_amount
    FROM hub_backlog_shipments b
    GROUP BY b.snapshot_date_key, b.hub_sk
)
SELECT
    g.snapshot_date_key,
    g.hub_sk,
    COALESCE(f.inbound_count, 0) AS inbound_count,
    COALESCE(f.outbound_count, 0) AS outbound_count,
    COALESCE(b.backlog_count, 0) AS backlog_count,
    COALESCE(b.pending_delivery_count, 0) AS pending_delivery_count,
    COALESCE(f.return_count, 0) AS return_count,
    COALESCE(b.cod_held_amount, 0) AS cod_held_amount,
    '{{ var("batch_id") }}' AS batch_id,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS snapshot_generated_at
FROM date_hub_grid g
LEFT JOIN hub_daily_flow f
  ON g.snapshot_date_key = f.snapshot_date_key AND g.hub_sk = f.hub_sk
LEFT JOIN hub_backlog_agg b
  ON g.snapshot_date_key = b.snapshot_date_key AND g.hub_sk = b.hub_sk
```

---

### 6.6 Tầng Semantic / Analytics (`dbt_project/models/gold/analytics/`)

#### File: `dbt_project/models/gold/analytics/v_slaship_summary.sql`

```sql
{{ config(materialized='view') }}

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
FROM {{ ref('fact_shipment_lifecycle') }} f
JOIN {{ ref('dim_date') }} d ON f.order_date_key = d.date_key
JOIN {{ ref('dim_service_type') }} st ON f.service_type_sk = st.service_type_sk
JOIN {{ ref('dim_hub') }} ho ON f.origin_hub_sk = ho.hub_sk
JOIN {{ ref('dim_hub') }} hd ON f.destination_hub_sk = hd.hub_sk
JOIN {{ ref('dim_customer') }} cs ON f.sender_customer_sk = cs.customer_sk
JOIN {{ ref('dim_status_reason') }} sr ON f.current_status_sk = sr.status_reason_sk
GROUP BY
    d.full_date, d.year, d.month, d.month_name,
    st.service_name, st.is_express,
    ho.hub_name, ho.city,
    hd.hub_name, hd.city,
    cs.customer_type, cs.membership_tier,
    sr.status_code, sr.reason_name
```

#### File: `dbt_project/models/gold/analytics/mv_daily_hub_performance.sql`

```sql
{{ config(materialized='table') }}

WITH ordered_hub_events AS (
    SELECT
        e.shipment_number,
        e.hub_sk,
        e.event_date_key,
        e.event_type,
        e.event_timestamp,
        LEAD(e.event_type) OVER (
            PARTITION BY e.shipment_number, e.hub_sk
            ORDER BY e.event_timestamp, e.event_sequence
        ) AS next_event_type,
        LEAD(e.event_timestamp) OVER (
            PARTITION BY e.shipment_number, e.hub_sk
            ORDER BY e.event_timestamp, e.event_sequence
        ) AS next_event_timestamp
    FROM {{ ref('fact_tracking_events') }} e
    WHERE e.event_type IN ('INBOUND_SCAN', 'OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
      AND e.hub_sk <> -1
),
hub_dwell AS (
    SELECT
        hub_sk,
        event_date_key AS snapshot_date_key,
        AVG(DATEDIFF('second', event_timestamp, next_event_timestamp) / 3600.0) AS avg_dwell_time_hours
    FROM ordered_hub_events
    WHERE event_type = 'INBOUND_SCAN'
      AND next_event_type IN ('OUTBOUND_SCAN', 'DELIVERED', 'RETURNED')
      AND next_event_timestamp >= event_timestamp
    GROUP BY hub_sk, event_date_key
)
SELECT
    d.full_date AS snapshot_date,
    d.year,
    d.month,
    h.hub_nk AS hub_code,
    h.hub_name,
    h.hub_level,
    h.city,
    h.district,
    h.capacity,
    s.inbound_count,
    s.outbound_count,
    s.backlog_count,
    s.pending_delivery_count,
    s.return_count,
    s.cod_held_amount,
    ROUND(COALESCE(dw.avg_dwell_time_hours, 0), 2) AS avg_dwell_time_hours,
    ROUND(100.0 * s.backlog_count / NULLIF(h.capacity, 0), 2) AS backlog_capacity_ratio_pct
FROM {{ ref('fact_daily_hub_snapshot') }} s
JOIN {{ ref('dim_date') }} d ON s.snapshot_date_key = d.date_key
JOIN {{ ref('dim_hub') }} h ON s.hub_sk = h.hub_sk
LEFT JOIN hub_dwell dw
  ON s.hub_sk = dw.hub_sk
 AND s.snapshot_date_key = dw.snapshot_date_key
WHERE h.hub_sk <> -1
```

#### File: `dbt_project/models/gold/analytics/mv_courier_performance.sql`

```sql
{{ config(materialized='table') }}

SELECT
    d.year,
    d.month,
    c.courier_sk,
    c.courier_nk AS courier_code,
    c.courier_name,
    c.contract_type,
    c.valid_from AS version_valid_from,
    c.valid_to AS version_valid_to,
    c.is_current AS is_current_version,
    c.assigned_hub_code AS assigned_hub_code_at_event,
    c.assigned_hub_name AS assigned_hub_at_event,
    c.assigned_hub_city AS assigned_hub_city,
    c.assigned_vehicle_type AS assigned_vehicle_type,
    COUNT(DISTINCT CASE
        WHEN e.event_type IN ('DELIVERED', 'RETURNED', 'DELIVERY_ATTEMPT') THEN e.shipment_number
    END) AS total_handled_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'PICKED_UP' THEN e.shipment_number
    END) AS total_picked_up_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'DELIVERED' THEN e.shipment_number
    END) AS delivered_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'RETURNED' THEN e.shipment_number
    END) AS returned_shipments,
    COUNT(DISTINCT CASE
        WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_number
    END) AS on_time_delivered_shipments,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' AND f.is_on_time THEN e.shipment_number END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type = 'DELIVERED' THEN e.shipment_number END), 0),
        2
    ) AS courier_otd_pct,
    ROUND(
        100.0 * COUNT(DISTINCT CASE WHEN e.event_type = 'RETURNED' THEN e.shipment_number END)
        / NULLIF(COUNT(DISTINCT CASE WHEN e.event_type IN ('DELIVERED', 'RETURNED') THEN e.shipment_number END), 0),
        2
    ) AS courier_return_rate_pct
FROM {{ ref('fact_tracking_events') }} e
JOIN {{ ref('fact_shipment_lifecycle') }} f ON e.shipment_number = f.shipment_number
JOIN {{ ref('dim_date') }} d ON e.event_date_key = d.date_key
JOIN {{ ref('dim_courier') }} c ON e.courier_sk = c.courier_sk
WHERE c.courier_sk <> -1
GROUP BY
    d.year, d.month,
    c.courier_sk, c.courier_nk, c.courier_name, c.contract_type,
    c.valid_from, c.valid_to, c.is_current,
    c.assigned_hub_code, c.assigned_hub_name, c.assigned_hub_city, c.assigned_vehicle_type
```

---

### 6.7 Kiểm thử Chất lượng Dữ liệu trên Silver & Gold (`dbt_project/models/schema.yml`)

```yaml
version: 2

models:
  - name: silver_shipments
    description: "Bảng vận đơn tầng Silver (đã lọc sạch mọi dòng Blocker vào META.DATA_QUARANTINE)"
    columns:
      - name: shipment_id
        tests:
          - not_null
          - unique
      - name: shipment_number
        tests:
          - not_null
          - unique
      - name: weight
        tests:
          - dbt_utils.expression_is_true:
              arguments:
                expression: "> 0"
      - name: current_status
        tests:
          - accepted_values:
              values: ['CREATED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERING', 'DELIVERY_FAILED', 'DELIVERED', 'RETURNED', 'CANCELLED']

  - name: dim_customer
    columns:
      - name: customer_sk
        tests:
          - not_null
          - unique

  - name: dim_courier
    columns:
      - name: courier_sk
        tests:
          - not_null
          - unique

  - name: fact_shipment_lifecycle
    columns:
      - name: shipment_id_nk
        tests:
          - not_null
          - unique
      - name: sender_customer_sk
        tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_sk
      - name: origin_hub_sk
        tests:
          - relationships:
              to: ref('dim_hub')
              field: hub_sk

  - name: fact_tracking_events
    columns:
      - name: event_id_nk
        tests:
          - not_null
          - unique
      - name: courier_sk
        tests:
          - relationships:
              to: ref('dim_courier')
              field: courier_sk
```

---

## 7. Nhóm Điều phối Pipeline Local & Airflow (`scripts/run_local_pipeline.py` & `dags/`)

### File: `scripts/run_local_pipeline.py`

```python
import sys
import uuid
import subprocess
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from sqlalchemy import text
from config.db_config import get_oltp_conn, get_snowflake_conn
from scripts.generate_oltp_data import generate_data
from scripts.extract_load_snowflake import run_extract_load_snowflake
from scripts.dq_gate_snowflake import run_dq_gate_snowflake
from scripts.post_load_watermark import update_watermark_on_success, mark_batch_failed

DBT_DIR = BASE_DIR / "dbt_project"


def execute_pg_sql_file(conn, file_path: Path):
    conn.execute(text(file_path.read_text(encoding="utf-8")))
    conn.commit()


def execute_sf_sql_file(sf_conn, file_path: Path):
    sql_text = file_path.read_text(encoding="utf-8")
    for cur in sf_conn.execute_string(sql_text):
        pass


def run_dbt_cmd(args: list[str]):
    cmd = ["dbt"] + args + ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    print(f"⚙️ Chạy lệnh dbt: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def init_databases():
    print("🛠️ Đang khởi tạo DDL cho PostgreSQL OLTP và Snowflake DWH...")
    with get_oltp_conn() as oltp_conn:
        execute_pg_sql_file(oltp_conn, BASE_DIR / "sql/oltp/01_create_oltp_tables.sql")
        execute_pg_sql_file(oltp_conn, BASE_DIR / "sql/oltp/02_seed_constraints.sql")

    with get_snowflake_conn(schema="PUBLIC") as sf_conn:
        execute_sf_sql_file(sf_conn, BASE_DIR / "sql/snowflake_init/01_init_snowflake.sql")


def run_pipeline():
    batch_id = f"BATCH_{uuid.uuid4().hex[:10].upper()}"
    dbt_vars = f'{{"batch_id": "{batch_id}"}}'
    print("=" * 75)
    print(f"🚀 KHỞI CHẠY SNOWFLAKE + DBT ELT PIPELINE | BATCH_ID: {batch_id}")
    print("=" * 75)

    init_databases()
    generate_data()

    try:
        # 1. Extract từ Postgres OLTP -> Load vào Snowflake STG
        run_extract_load_snowflake(batch_id)

        # 2. Chạy Data Quality Gate & cách ly lỗi vào META.DATA_QUARANTINE
        run_dq_gate_snowflake(batch_id)

        # 3. Cài đặt dbt packages & chạy tầng Silver
        run_dbt_cmd(["deps"])
        run_dbt_cmd(["run", "--select", "silver", "--vars", dbt_vars])

        # 4. Chạy dbt snapshot để ghi nhận lịch sử SCD Type 2 (Customers & Couriers)
        run_dbt_cmd(["snapshot", "--vars", dbt_vars])

        # 5. Chạy tầng Gold (8 Dimensions -> 3 Facts -> 3 Views/MVs)
        run_dbt_cmd(["run", "--select", "gold.dimensions", "--vars", dbt_vars])
        run_dbt_cmd(["run", "--select", "gold.facts", "--vars", dbt_vars])
        run_dbt_cmd(["run", "--select", "gold.analytics", "--vars", dbt_vars])

        # 6. Kiểm tra chất lượng dữ liệu đầu ra bằng dbt test
        run_dbt_cmd(["test", "--vars", dbt_vars])

        # 7. Cập nhật Watermark từ SOURCE_SNAPSHOT_TS sau khi toàn bộ pipeline thành công
        update_watermark_on_success(batch_id)

    except Exception as exc:
        mark_batch_failed(batch_id, repr(exc))
        print(f"❌ Batch {batch_id} THẤT BẠI: {exc}")
        raise

    with get_snowflake_conn(schema="GOLD") as sf_conn:
        cur = sf_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM GOLD.FACT_SHIPMENT_LIFECYCLE")
        fsl_cnt = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM GOLD.FACT_TRACKING_EVENTS")
        fte_cnt = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM GOLD.FACT_DAILY_HUB_SNAPSHOT")
        fdhs_cnt = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM META.DATA_QUARANTINE")
        quar_cnt = cur.fetchone()[0]
        print("=" * 75)
        print("🎉 SNOWFLAKE + DBT PIPELINE HOÀN TẤT THÀNH CÔNG!")
        print(f"   • GOLD.FACT_SHIPMENT_LIFECYCLE : {fsl_cnt:,} dòng")
        print(f"   • GOLD.FACT_TRACKING_EVENTS    : {fte_cnt:,} dòng")
        print(f"   • GOLD.FACT_DAILY_HUB_SNAPSHOT : {fdhs_cnt:,} dòng")
        print(f"   • META.DATA_QUARANTINE         : {quar_cnt:,} bản ghi lỗi/cảnh báo đã bắt được")
        print("=" * 75)


if __name__ == "__main__":
    run_pipeline()
```

### File: `dags/express_delivery_snowflake_dbt_dag.py`

```python
import sys
import hashlib
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.db_config import get_oltp_conn, get_snowflake_conn
from scripts.extract_load_snowflake import run_extract_load_snowflake
from scripts.dq_gate_snowflake import run_dq_gate_snowflake
from scripts.post_load_watermark import update_watermark_on_success, mark_batch_failed

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"


def _batch_id(context) -> str:
    run_id = context["run_id"]
    batch_id = f"AF_{run_id}"
    return batch_id if len(batch_id) <= 64 else "AF_" + hashlib.md5(run_id.encode("utf-8")).hexdigest()


def _on_task_failure(context):
    mark_batch_failed(_batch_id(context), str(context.get("exception")))


def task_preflight_checks(**context):
    with get_oltp_conn() as oltp_conn:
        oltp_conn.execute(text("SELECT 1 FROM shipments LIMIT 1"))
    with get_snowflake_conn(schema="META") as sf_conn:
        sf_conn.cursor().execute("SELECT 1 FROM META.ETL_WATERMARK LIMIT 1")
    print("✅ Preflight checks: Kết nối Postgres OLTP và Snowflake META đều sẵn sàng.")


def task_extract_load_raw(**context):
    batch_id = _batch_id(context)
    context["ti"].xcom_push(key="batch_id", value=batch_id)
    run_extract_load_snowflake(batch_id)


def task_dq_gate(**context):
    run_dq_gate_snowflake(_batch_id(context))


def task_update_watermark(**context):
    update_watermark_on_success(_batch_id(context))


default_args = {
    "owner": "express_dwh_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": _on_task_failure,
}

with DAG(
    dag_id="express_delivery_snowflake_elt",
    default_args=default_args,
    description="Modern ELT Pipeline (Postgres OLTP -> Snowflake + dbt) cho Express Delivery DWH",
    schedule="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["express_delivery", "snowflake", "dbt", "medallion", "kimball", "scd2"],
) as dag:

    preflight_checks = PythonOperator(
        task_id="preflight_checks",
        python_callable=task_preflight_checks,
    )

    extract_and_load_raw = PythonOperator(
        task_id="extract_postgres_to_snowflake_stg",
        python_callable=task_extract_load_raw,
    )

    data_quality_gate = PythonOperator(
        task_id="snowflake_data_quality_gate",
        python_callable=task_dq_gate,
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            "--select silver --vars '{\"batch_id\": \"{{ ti.xcom_pull(task_ids=\"extract_postgres_to_snowflake_stg\", key=\"batch_id\") }}\"}'"
        ),
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_scd2",
        bash_command=(
            f"dbt snapshot --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            "--vars '{\"batch_id\": \"{{ ti.xcom_pull(task_ids=\"extract_postgres_to_snowflake_stg\", key=\"batch_id\") }}\"}'"
        ),
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold_marts",
        bash_command=(
            f"dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            "--select gold.dimensions gold.facts gold.analytics "
            "--vars '{\"batch_id\": \"{{ ti.xcom_pull(task_ids=\"extract_postgres_to_snowflake_stg\", key=\"batch_id\") }}\"}'"
        ),
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=(
            f"dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} "
            "--vars '{\"batch_id\": \"{{ ti.xcom_pull(task_ids=\"extract_postgres_to_snowflake_stg\", key=\"batch_id\") }}\"}'"
        ),
    )

    finalize_watermark = PythonOperator(
        task_id="update_watermark_and_audit_log",
        python_callable=task_update_watermark,
    )

    (
        preflight_checks
        >> extract_and_load_raw
        >> data_quality_gate
        >> dbt_run_silver
        >> dbt_snapshot
        >> dbt_run_gold
        >> dbt_test_marts
        >> finalize_watermark
    )
```

---

## 8. Đóng gói Docker Compose (`docker/`)

### File: `docker/requirements.txt`

```text
SQLAlchemy>=1.4.49,<2.0
psycopg2-binary==2.9.9
pandas==2.1.4
Faker==25.2.0
snowflake-connector-python[pandas]==3.12.2
dbt-core==1.8.7
dbt-snowflake==1.8.4
```

### File: `docker/init-db.sql`

```sql
CREATE DATABASE express_oltp;
```

### File: `docker/docker-compose.yml`

```yaml
services:
  postgres-oltp:
    image: postgres:15
    container_name: express_postgres_oltp
    environment:
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=postgres
      - POSTGRES_DB=airflow_meta
    ports:
      - "5432:5432"
    volumes:
      - pg_oltp_data:/var/lib/postgresql/data
      - ./init-db.sql:/docker-entrypoint-initdb.d/01_init_db.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "psql -U postgres -d express_oltp -c 'SELECT 1' > /dev/null 2>&1"]
      interval: 5s
      timeout: 5s
      retries: 20

  airflow:
    image: apache/airflow:2.9.1-python3.11
    container_name: express_airflow
    depends_on:
      postgres-oltp:
        condition: service_healthy
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:postgres@postgres-oltp:5432/airflow_meta
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - OLTP_DB_URI=postgresql+psycopg2://postgres:postgres@postgres-oltp:5432/express_oltp
      - SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}
      - SNOWFLAKE_USER=${SNOWFLAKE_USER}
      - SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}
      - SNOWFLAKE_ROLE=${SNOWFLAKE_ROLE:-ACCOUNTADMIN}
      - SNOWFLAKE_WAREHOUSE=${SNOWFLAKE_WAREHOUSE:-EXPRESS_WH}
      - SNOWFLAKE_DATABASE=${SNOWFLAKE_DATABASE:-EXPRESS_DWH}
      - PYTHONPATH=/opt/airflow
    volumes:
      - ../dags:/opt/airflow/dags
      - ../scripts:/opt/airflow/scripts
      - ../config:/opt/airflow/config
      - ../sql:/opt/airflow/sql
      - ../dbt_project:/opt/airflow/dbt_project
      - ./requirements.txt:/opt/airflow/requirements.txt
    ports:
      - "8080:8080"
    command: >
      bash -c "
      pip install --no-cache-dir -r /opt/airflow/requirements.txt &&
      export PYTHONPATH=/opt/airflow &&
      airflow db migrate &&
      (airflow users create --username admin --password admin --firstname Express --lastname Admin --role Admin --email admin@express.vn || true) &&
      (python /opt/airflow/scripts/run_local_pipeline.py || echo '⚠️ Pipeline khởi tạo lỗi - kiểm tra biến môi trường Snowflake và bảng META.ETL_BATCH_LOG') ;
      airflow webserver &
      exec airflow scheduler
      "

volumes:
  pg_oltp_data:
```

---

## 9. Truy vấn đối soát nhanh trên Snowflake (Kỳ vọng kết quả y hệt bản cũ

)

Sau khi khai báo biến môi trường Snowflake (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`) và chạy `python scripts/run_local_pipeline.py` (lần 1: Full Load, lần 2: Incremental + SCD2), chạy 4 câu truy vấn đối soát dưới đây trên Snowflake Worksheet:

```sql
USE DATABASE EXPRESS_DWH;

-- 1. Không còn bản ghi "mất im lặng" giữa STG và SILVER (Kỳ vọng = 0)
SELECT COUNT(*) AS MAT_IM_LANG
FROM STG.SHIPMENT_TRACKING_EVENTS e
WHERE NOT EXISTS (SELECT 1 FROM SILVER.SILVER_SHIPMENT_TRACKING_EVENTS s WHERE s.EVENT_ID = e.EVENT_ID)
  AND NOT EXISTS (
      SELECT 1 FROM META.DATA_QUARANTINE q
      WHERE q.SOURCE_TABLE = 'shipment_tracking_events'
        AND q.SEVERITY = 'Blocker'
        AND q.RECORD_ID = TO_VARCHAR(e.EVENT_ID)
  );

-- 2. Trạng thái hiện tại trên Fact khớp 100% với cờ nghiệp vụ (Kỳ vọng = 0)
SELECT COUNT(*) AS LECH_TRANG_THAI
FROM GOLD.FACT_SHIPMENT_LIFECYCLE f
JOIN GOLD.DIM_STATUS_REASON sr ON sr.STATUS_REASON_SK = f.CURRENT_STATUS_SK
WHERE (f.IS_DELIVERED AND sr.STATUS_CODE <> 'DELIVERED')
   OR (f.IS_RETURNED  AND sr.STATUS_CODE <> 'RETURNED')
   OR (f.IS_CANCELLED AND sr.STATUS_CODE <> 'CANCELLED');

-- 3. Mọi Fact trỏ đúng phiên bản SCD2 bao trùm thời điểm phát sinh (Kỳ vọng = 0, 0)
SELECT
    (SELECT COUNT(*) FROM GOLD.FACT_TRACKING_EVENTS e
     JOIN GOLD.DIM_COURIER c ON c.COURIER_SK = e.COURIER_SK
     WHERE NOT (e.EVENT_TIMESTAMP >= c.VALID_FROM AND e.EVENT_TIMESTAMP < c.VALID_TO)) AS SU_KIEN_SAI_PHIEN_BAN,
    (SELECT COUNT(*) FROM GOLD.FACT_SHIPMENT_LIFECYCLE f
     JOIN GOLD.DIM_CUSTOMER c ON c.CUSTOMER_SK IN (f.SENDER_CUSTOMER_SK, f.RECEIVER_CUSTOMER_SK)
     WHERE NOT (f.ORDER_TIMESTAMP >= c.VALID_FROM AND f.ORDER_TIMESTAMP < c.VALID_TO)) AS VAN_DON_SAI_PHIEN_BAN;

-- 4. Đơn đã giao: đúng hạn + trễ hạn = tổng đã giao (Kỳ vọng 2 cột bằng nhau)
SELECT
    COUNT_IF(IS_DELIVERED) AS DELIVERED,
    COUNT_IF(IS_ON_TIME) + COUNT_IF(IS_SLA_BREACHED) AS ON_TIME_PLUS_BREACHED
FROM GOLD.FACT_SHIPMENT_LIFECYCLE;
```
