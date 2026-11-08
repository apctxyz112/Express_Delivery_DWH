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