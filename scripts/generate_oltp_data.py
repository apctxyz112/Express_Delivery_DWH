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