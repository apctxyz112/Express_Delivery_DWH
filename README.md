# Express Delivery Data Warehouse

Repo chứa toàn bộ pipeline ELT cho bài toán chuyển phát nhanh: PostgreSQL làm nguồn OLTP, Snowflake làm DWH đích, dbt transform, Airflow điều phối. Chạy local bằng Docker, đẩy dữ liệu lên Snowflake qua API.

Mục tiêu là có một DWH theo kiến trúc Medallion (STG → Silver → Gold), dùng mô hình Kimball Star Schema, có SCD Type 2 cho khách hàng và bưu tá, và có Data Quality Gate chặn dữ liệu lỗi trước khi vào tầng phân tích.

---

## Kiến trúc

```
[ LOCAL / DOCKER ]                          [ CLOUD: SNOWFLAKE ]
┌───────────────────────┐                   ┌──────────────────────────┐
│  PostgreSQL 15        │                   │  STG ──► SILVER ──► GOLD │
│  11 bảng, FK+CHECK    │                   │  (raw)   (clean+   (dim/ │
│  Sinh dữ liệu sạch    │                   │           SCD2)    fact) │
└──────────┬────────────┘                   │  META: watermark + batch │
           │                                │        log + quarantine  │
           ▼ read_sql / write_pandas        └──────────────────────────┘
┌───────────────────────┐                           ▲
│  Airflow (scheduler)  │ ── dbt CLI gọi API ──────┘
│  Python EL + DQ Gate  │
│  Chaos Injection (STG)│
└───────────────────────┘
```

**Chạy local:** PostgreSQL 15 (11 bảng nghiệp vụ với đầy đủ FK, CHECK, UNIQUE, trigger `updated_at`, index) và Airflow (scheduler + webserver). dbt-core cũng chạy local nhưng chỉ đóng vai trò client — biên dịch SQL/Jinja rồi bắn lên Snowflake, không xử lý dữ liệu tại chỗ.

**Chạy cloud:** Snowflake với 4 schema — `STG` (raw), `SILVER` (clean + SCD2 snapshots), `GOLD` (8 dims, 3 facts, 3 analytics), `META` (watermark, batch log, quarantine). Toàn bộ compute chạy trên warehouse `EXPRESS_WH`.

**Xử lý dữ liệu lỗi:** OLTP giữ sạch 100% (mọi CHECK constraint đều bật). Muốn test DQ Gate thì bật `SIMULATE_STG_CHAOS=true` — script extract sẽ tiêm lỗi vào DataFrame trước khi nạp vào STG. Blocker bị cách ly vào `META.DATA_QUARANTINE`, Silver lọc bỏ qua `NOT EXISTS`.

---

## Cấu trúc thư mục

```
express-delivery-dwh/
├── .env                                    # Biến môi trường (không commit)
├── config/
│   ├── db_config.py                        # SQLAlchemy (OLTP) + Snowflake connector
│   └── settings.py                         # Đọc env, quy mô sinh dữ liệu
├── sql/
│   ├── oltp/
│   │   ├── 01_create_oltp_tables.sql       # DDL 11 bảng
│   │   └── 02_seed_constraints.sql         # FK, CHECK, trigger, index
│   └── snowflake_init/
│       └── 01_init_snowflake.sql           # Warehouse, DB, 4 schema, META/STG, UDF
├── scripts/
│   ├── generate_oltp_data.py               # Sinh dữ liệu sạch + incremental SCD2
│   ├── extract_load_snowflake.py           # Extract REPEATABLE READ → STG + chaos
│   ├── dq_gate_snowflake.py                # 14 rule DQ → DATA_QUARANTINE
│   ├── post_load_watermark.py              # Monotonic watermark + đóng batch
│   └── run_local_pipeline.py               # Orchestrator local (hỗ trợ CLI flags)
├── dbt_project/
│   ├── dbt_project.yml, packages.yml, profiles.yml
│   ├── macros/generate_schema_name.sql
│   ├── snapshots/{snap_customers, snap_couriers}.sql
│   └── models/
│       ├── sources.yml, schema.yml
│       ├── silver/                         # 11 models
│       └── gold/
│           ├── dimensions/                 # 8 dims
│           ├── facts/                      # 3 facts
│           └── analytics/                  # 3 views/MVs
├── dags/express_delivery_snowflake_dbt_dag.py
└── docker/{docker-compose.yml, init-db.sql, requirements.txt}
```

---

## Chuẩn bị

Cần có:

- Python 3.10+ (64-bit)
- Docker Desktop
- Tài khoản Snowflake (free trial cũng được)

Kiểm tra Python:

```powershell
python -c "import struct; print(struct.calcsize('P') * 8)"
```

Phải ra `64`. Nếu ra `32` thì cài lại bản 64-bit.

---

## Cài đặt lần đầu

### 1. Khởi tạo Snowflake

Đăng ký tài khoản ở `signup.snowflake.com`, chọn Enterprise Edition, region gần VN nhất (thường `ap-southeast-1`).

Lấy Account Identifier: Snowsight → click tên profile góc dưới trái → Account → copy. Định dạng sẽ là một trong hai:

- `xy12345.ap-southeast-1` (nếu URL có region)
- `org-account` (nếu URL kiểu tổ chức)

Cắt bỏ `https://` và `.snowflakecomputing.com` nếu có.

Mở Snowflake Worksheet, paste toàn bộ nội dung `sql/snowflake_init/01_init_snowflake.sql` rồi Run. Script tạo warehouse `EXPRESS_WH`, database `EXPRESS_DWH` với 4 schema, 3 bảng META, 11 bảng STG, và function `GOLD.FN_DATE_KEY`.

### 2. Tạo file `.env`

Ở thư mục gốc:

```ini
OLTP_DB_URI=postgresql+psycopg2://postgres:postgres@postgres-oltp:5432/express_oltp
SNOWFLAKE_ACCOUNT=...
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=EXPRESS_WH
SNOWFLAKE_DATABASE=EXPRESS_DWH
SIMULATE_STG_CHAOS=true
```

`postgres-oltp` là hostname nội bộ Docker. Khi chạy script Python trực tiếp trên máy thật (không qua container) thì phải đổi thành `localhost`.

### 3. Cài môi trường Python

```powershell
cd express-delivery-dwh
python -m venv venv
.\venv\Scripts\Activate.ps1
# Nếu lỗi ExecutionPolicy: Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
python -m pip install --upgrade pip setuptools wheel
pip install -r docker/requirements.txt
```

Nạp `.env` vào PowerShell rồi test kết nối dbt:

```powershell
Get-Content .env | Where-Object { $_ -match '^[^#].*=.*' } | ForEach-Object {
    $parts = $_ -split '=', 2
    [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
}

cd dbt_project
dbt deps --profiles-dir .
dbt debug --profiles-dir .
cd ..
```

Ra `All checks passed! Connection test: [OK connection ok]` là dbt đã nối được Snowflake.

### 4. Dọn Docker cũ (nếu có)

```powershell
docker compose -f docker/docker-compose.yml down -v --remove-orphans
docker ps -aq | ForEach-Object { docker rm -f $_ }
docker volume ls -q | ForEach-Object { docker volume rm $_ }
docker system prune -a --volumes -f
```

### 5. Khởi động PostgreSQL, tạo schema, sinh dữ liệu

Bước này chạy Python trên máy thật nên cần đổi host về `localhost`.

```powershell
# Chỉ bật Postgres, chưa bật Airflow
docker compose -f docker/docker-compose.yml up -d postgres-oltp

# Đợi đến khi log ra "database system is ready to accept connections"
docker logs -f express_postgres_oltp
```

Ctrl+C thoát log. Kiểm tra database `express_oltp`:

```powershell
docker exec -it express_postgres_oltp psql -U postgres -c "\l"
```

Nếu chưa có thì tạo:

```powershell
docker exec -it express_postgres_oltp psql -U postgres -c "CREATE DATABASE express_oltp;"
```

Kích hoạt venv, nạp `.env`, override URI về localhost:

```powershell
.\venv\Scripts\Activate.ps1

Get-Content .env | Where-Object { $_ -match '^[^#].*=.*' } | ForEach-Object {
    $parts = $_ -split '=', 2
    [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
}
$env:OLTP_DB_URI="postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
```

Test kết nối:

```powershell
python  test_conn.py
```

Phải ra `PostgreSQL 15.x`. Nếu lỗi `Connection refused` thì container chưa sẵn sàng, quay lại chờ log.

Tạo 11 bảng và constraints:

```powershell
python -c "
from pathlib import Path
from sqlalchemy import text
from config.db_config import get_oltp_conn

with get_oltp_conn() as conn:
    for f in ['sql/oltp/01_create_oltp_tables.sql', 'sql/oltp/02_seed_constraints.sql']:
        print(f'Executing {f}')
        conn.execute(text(Path(f).read_text(encoding='utf-8')))
    conn.commit()
    print('OLTP schema created')
"
```

Sinh dữ liệu lần đầu:

```powershell
python -c "from scripts.generate_oltp_data import generate_data; generate_data()"
```

Script log ra `Khởi tạo dữ liệu OLTP lần đầu (10000 vận đơn + danh mục chuẩn)`. Mất khoảng 1-3 phút, sinh 1500 customers, 30 hubs, 80 vehicles, 200 couriers, 10000 shipments cùng tracking events, cod settlements, customer tickets.


### 6. Bật Airflow và chạy DAG

```powershell
docker compose -f docker/docker-compose.yml --env-file .env up -d
docker logs -f express_airflow
```

Vào http://localhost:8080, đăng nhập `admin` / `admin`. Tìm DAG `express_delivery_snowflake_elt`, gạt **Unpause** rồi bấm **Trigger DAG** để chạy batch đầu. Nếu để Unpause thì Airflow tự chạy lại mỗi 4 tiếng (`0 */4 * * *`).

Đợi DAG xong, log sẽ ra thông báo pipeline hoàn tất kèm số dòng từng fact.

### 7. Kiểm tra kết quả trên Snowflake

Mở Snowflake Worksheet:

```sql
USE DATABASE EXPRESS_DWH;

SELECT 'STG' AS layer, COUNT(*) FROM STG.SHIPMENTS
UNION ALL SELECT 'SILVER', COUNT(*) FROM SILVER.SILVER_SHIPMENTS
UNION ALL SELECT 'GOLD (fact)', COUNT(*) FROM GOLD.FACT_SHIPMENT_LIFECYCLE
UNION ALL SELECT 'QUARANTINE (Blocker)', COUNT(*) FROM META.DATA_QUARANTINE WHERE SEVERITY = 'Blocker';
```

Số dòng STG phải bằng số shipments trong OLTP. Silver ≤ STG (đã trừ Blocker). Gold phải có dữ liệu.

---

## Chạy lại trên máy đã setup

Sau lần đầu, dữ liệu đã có nên có thể bỏ qua vài bước cho nhanh. Chạy incremental bình thường (sinh thêm 200 vận đơn mới + cập nhật SCD2):

```powershell
python scripts/run_local_pipeline.py
```

Các cờ hay dùng:

| Cờ                 | Tác dụng                               | Khi nào dùng                                |
| ------------------- | ---------------------------------------- | --------------------------------------------- |
| `--skip-init`     | Bỏ qua tạo DDL                         | DB đã có sẵn                              |
| `--skip-generate` | Không sinh dữ liệu mới               | Chỉ chạy lại ELT trên dữ liệu hiện có |
| `--full-refresh`  | Thêm `--full-refresh` cho `dbt run` | Rebuild toàn bộ Silver + Gold               |
| `--skip-test`     | Bỏ qua `dbt test`                     | Chạy nhanh khi debug                         |

Ví dụ chạy ELT không sinh dữ liệu mới, không tạo lại DDL:

```powershell
python scripts/run_local_pipeline.py --skip-init --skip-generate
```

Chạy trong container Airflow:

```powershell
docker exec -it express_airflow python /opt/airflow/scripts/run_local_pipeline.py --skip-init
```

### Chạy riêng từng tầng dbt khi debug

```powershell
cd dbt_project

dbt run --select silver --vars '{"batch_id": "MANUAL_BATCH"}' --profiles-dir .
dbt snapshot --vars '{"batch_id": "MANUAL_BATCH"}' --profiles-dir .
dbt run --select gold.dimensions --vars '{"batch_id": "MANUAL_BATCH"}' --profiles-dir .
dbt run --select gold.facts --vars '{"batch_id": "MANUAL_BATCH"}' --profiles-dir .
dbt run --select gold.analytics --vars '{"batch_id": "MANUAL_BATCH"}' --profiles-dir .
dbt test --vars '{"batch_id": "MANUAL_BATCH"}' --profiles-dir .
```

Không truyền `--vars` hoặc truyền `MANUAL_BATCH` thì dbt sẽ quét toàn bộ STG thay vì lọc theo batch cụ thể. Tiện khi debug hoặc backfill.

### Mở khóa batch khi pipeline bị ngắt

Nếu lần chạy trước bị kill giữa chừng, batch có thể kẹt ở `RUNNING` hoặc `EXTRACTED`. Trên Snowflake:

```sql
UPDATE EXPRESS_DWH.META.ETL_BATCH_LOG
SET STATUS = 'FAILED', RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
WHERE STATUS IN ('RUNNING', 'EXTRACTED');
```

Rồi chạy lại pipeline bình thường.

---

## Pipeline chạy gì

Cả `run_local_pipeline.py` và Airflow DAG đều chạy cùng 11 bước:

```
preflight
  → extract_postgres_to_snowflake_stg
    → snowflake_data_quality_gate
      → dq_gate_blocker_check
        → dbt_run_silver
          → dbt_snapshot_scd2
            → dbt_run_gold_dimensions
              → dbt_run_gold_facts
                → dbt_run_gold_analytics
                  → dbt_test_marts
                    → update_watermark_and_audit_log
```

1. **Preflight** — kiểm tra kết nối Postgres + Snowflake META.
2. **Extract** — mở `REPEATABLE READ` trên Postgres, lấy `snapshot_ts`, đọc 11 bảng với điều kiện `updated_at > watermark - 10 phút AND <= snapshot_ts`. Nếu `SIMULATE_STG_CHAOS=true` thì tiêm lỗi vào DataFrame trước khi ghi STG.
3. **DQ Gate** — chạy 14 rule, ghi bản ghi lỗi vào `META.DATA_QUARANTINE` (Blocker hoặc Warning).
4. **Blocker check** — đếm Blocker trước khi qua Silver (chỉ log warning, không chặn).
5. **dbt Silver** — 11 models, dedup bằng `QUALIFY ROW_NUMBER()`, lọc bỏ Blocker, chuẩn hóa chuỗi, tính `row_hash`.
6. **dbt Snapshot** — chạy `snap_customers` và `snap_couriers` theo strategy `check` trên `row_hash`.
7. **Gold Dimensions** — build 8 dims với Unknown row (SK=-1) và `valid_to`/`valid_from` liền kề.
8. **Gold Facts** — build 3 facts, point-in-time join vào SCD2.
9. **Gold Analytics** — build 3 views/MVs.
10. **dbt Test** — chạy toàn bộ test trong `schema.yml`. Test fail là cảnh báo nghiệp vụ, không chặn watermark.
11. **Watermark** — `LAST_EXTRACTED_TIMESTAMP = SOURCE_SNAPSHOT_TS` (GREATEST để không lùi), đóng batch `SUCCESS`.

---

## Audit dữ liệu trên Snowflake

Vài query kiểm tra tính toàn vẹn:

```sql
USE DATABASE EXPRESS_DWH;

-- Không có bản ghi STG nào mất im lặng (không vào Silver cũng không vào Quarantine)
SELECT COUNT(*) AS lost_records
FROM STG.SHIPMENT_TRACKING_EVENTS e
WHERE NOT EXISTS (SELECT 1 FROM SILVER.SILVER_SHIPMENT_TRACKING_EVENTS s WHERE s.EVENT_ID = e.EVENT_ID)
  AND NOT EXISTS (
      SELECT 1 FROM META.DATA_QUARANTINE q
      WHERE q.SOURCE_TABLE = 'shipment_tracking_events'
        AND q.SEVERITY = 'Blocker'
        AND q.RECORD_ID = TO_VARCHAR(e.EVENT_ID)
  );

-- Fact status khớp với dimension
SELECT COUNT(*) AS status_mismatch
FROM GOLD.FACT_SHIPMENT_LIFECYCLE f
JOIN GOLD.DIM_STATUS_REASON sr ON sr.STATUS_REASON_SK = f.CURRENT_STATUS_SK
WHERE (f.IS_DELIVERED AND sr.STATUS_CODE <> 'DELIVERED')
   OR (f.IS_RETURNED AND sr.STATUS_CODE <> 'RETURNED')
   OR (f.IS_CANCELLED AND sr.STATUS_CODE <> 'CANCELLED');

-- Fact trỏ đúng phiên bản SCD2 tại thời điểm giao dịch
SELECT
    (SELECT COUNT(*) FROM GOLD.FACT_TRACKING_EVENTS e
     JOIN GOLD.DIM_COURIER c ON c.COURIER_SK = e.COURIER_SK
     WHERE NOT (e.EVENT_TIMESTAMP >= c.VALID_FROM AND e.EVENT_TIMESTAMP < c.VALID_TO)) AS wrong_scd_version_events,
    (SELECT COUNT(*) FROM GOLD.FACT_SHIPMENT_LIFECYCLE f
     JOIN GOLD.DIM_CUSTOMER c ON c.CUSTOMER_SK IN (f.SENDER_CUSTOMER_SK, f.RECEIVER_CUSTOMER_SK)
     WHERE NOT (f.ORDER_TIMESTAMP >= c.VALID_FROM AND f.ORDER_TIMESTAMP < c.VALID_TO)) AS wrong_scd_version_shipments;

-- On-time + breach phải bằng delivered
SELECT
    COUNT_IF(IS_DELIVERED) AS delivered,
    COUNT_IF(IS_ON_TIME) + COUNT_IF(IS_SLA_BREACHED) AS ontime_plus_breach
FROM GOLD.FACT_SHIPMENT_LIFECYCLE;

-- SCD2 không có khe hở thời gian
SELECT COUNT(*) AS scd2_gap
FROM (
    SELECT courier_nk, valid_to,
           LEAD(valid_from) OVER (PARTITION BY courier_nk ORDER BY valid_from) AS next_valid_from
    FROM GOLD.DIM_COURIER WHERE courier_sk <> -1
) t
WHERE valid_to <> next_valid_from AND next_valid_from IS NOT NULL;
```

---

## Lệnh quản trị

| Việc                             | Lệnh                                                                                                     |
| --------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Bật toàn bộ Docker             | `docker compose -f docker/docker-compose.yml --env-file .env up -d`                                     |
| Xem log Airflow                   | `docker logs -f express_airflow`                                                                        |
| Chạy incremental trong container | `docker exec -it express_airflow python /opt/airflow/scripts/run_local_pipeline.py --skip-init`         |
| Full refresh dbt                  | `python scripts/run_local_pipeline.py --skip-init --skip-generate --full-refresh`                       |
| Chạy `dbt test` riêng         | `docker exec -it express_airflow bash -c "cd /opt/airflow/dbt_project && dbt test --profiles-dir ."`    |
| Xem SQL dbt biên dịch           | `docker exec -it express_airflow bash -c "cd /opt/airflow/dbt_project && dbt compile --profiles-dir ."` |
| Dừng (giữ data)                 | `docker compose -f docker/docker-compose.yml down`                                                      |
| Xóa sạch container + volume     | `docker compose -f docker/docker-compose.yml down -v`                                                   |

---

## Xử lý sự cố

| Lỗi                                      | Nguyên nhân                        | Xử lý                                                                       |
| ----------------------------------------- | ------------------------------------ | ----------------------------------------------------------------------------- |
| `Database 'EXPRESS_DWH' does not exist` | Chưa chạy init SQL trên Snowflake | Chạy file `01_init_snowflake.sql` rồi `docker restart express_airflow`  |
| `Connection refused` khi test Postgres  | Container chưa ready                | Chờ log `database system is ready`                                         |
| `Batch ... đang chạy dở`             | Batch trước bị kill giữa chừng  | Chạy UPDATE ở mục "Mở khóa batch"                                        |
| `dbt debug` fail                        | Biến Snowflake sai                  | Kiểm tra `.env`, đặc biệt `SNOWFLAKE_ACCOUNT` không có `https://` |
| Pipeline chạy nhưng Silver 0 dòng      | `batch_id` không khớp            | Xem log extract, đảm bảo batch_id truyền đúng                           |
| Weight = -2.5 ở Quarantine               | Đúng thiết kế (chaos injection)  | Tắt bằng `SIMULATE_STG_CHAOS=false`                                       |
| SCD2 không phát hiện thay đổi        | `row_hash` không đổi            | Kiểm tra `_simulate_incremental_batch` có update cột trong `row_hash`  |
| Airflow UI không vào được            | Port 8080 bị chiếm                 | Đổi port trong `docker-compose.yml`                                       |
| `dbt test` fail nhưng pipeline SUCCESS | Test fail = warning, không chặn    | Xem log task `dbt_test_marts` để biết test nào fail                     |

---
