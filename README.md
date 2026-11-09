
# Express Delivery DWH: Kiến Trúc Lai (Hybrid Modern ELT)

**Local Orchestration (Docker, PostgreSQL 15, Apache Airflow, dbt Core) $\longleftrightarrow$ Cloud Data Warehouse (Snowflake)**[cite: 1, 2]

Dự án xây dựng hệ thống Kho dữ liệu (Data Warehouse) theo kiến trúc **Medallion (STG $\rightarrow$ SILVER $\rightarrow$ GOLD)** kết hợp mô hình chiều **Kimball Star Schema** và lịch sử thay đổi **SCD Type 2** cho bài toán Giao nhận Chuyển phát nhanh (Express Delivery)[cite: 1, 2].

---

## 1. Tổng Quan Kiến Trúc Lai (Hybrid Architecture)

Hệ thống phân tách rõ ràng giữa tầng điều phối cục bộ và tầng tính toán đám mây theo mô hình **Hybrid Modern ELT**[cite: 2]:

```text
[ LOCAL MACHINE / DOCKER ]                                     [ CLOUD (AWS / AZURE / GCP) ]
┌─────────────────────────┐                                   ┌──────────────────────────────────┐
│  PostgreSQL 15 (OLTP)   │                                   │       Snowflake Data Cloud       │
│  - Chứa DB nghiệp vụ    │                                   │                                  │
│  - Sinh dữ liệu giao dịch│                                  │  ┌────────────┐   ┌───────────┐  │
└────────────┬────────────┘                                   │  │ STG (Raw)  │──>│  SILVER   │  │
             │                                                │  └────────────┘   └─────┬─────┘  │
             ▼ (read_sql / write_pandas qua Internet)         │                         │        │
┌─────────────────────────┐                                   │                         ▼        │
│ Apache Airflow (Docker) │ ──[ Bắn lệnh dbt biên dịch ]────> │  ┌────────────┐   ┌───────────┐  │
│  - Điều phối lịch trình │                                   │  │ GOLD (Mart)│<──│ SCD2 Snap │  │
│  - Python EL & DQ Gate  │                                   │  └────────────┘   └───────────┘  │
│  - dbt-core CLI runner  │                                   │  ┌───────────────────────────┐   │
└─────────────────────────┘                                   │  │ META (Watermark, Quarant) │   │
                                                              │  └───────────────────────────┘   │
                                                              │   (100% Compute & Storage Cloud) │
                                                              └──────────────────────────────────┘
```

### Phân định trách nhiệm tài nguyên:

* **Tại máy cục bộ (Local Container via Docker)**[cite: 2]:
  * Lưu trữ cơ sở dữ liệu giao dịch nguồn `express_oltp` (PostgreSQL 15) gồm 11 bảng nghiệp vụ[cite: 1, 2].
  * Vận hành Apache Airflow (`Scheduler`, `Webserver`, `airflow_meta` DB) để lập lịch và điều phối pipeline[cite: 1, 2].
  * Thực thi `dbt-core` và `dbt-snowflake` với vai trò **client**: biên dịch mã SQL/Jinja và đẩy truy vấn qua API lên Snowflake (máy local không tốn RAM/CPU để xử lý biến đổi dữ liệu lớn)[cite: 2].
* **Tại hạ tầng đám mây (Snowflake Data Cloud)**[cite: 2]:
  * Lưu trữ 100% dữ liệu qua 4 Schemas: `STG` (Bronze/Raw), `SILVER` (Cleaned & Deduplicated + SCD2 Snapshots), `GOLD` (8 Dimensions, 3 Facts, 3 Views/MVs) và `META` (Watermark, Batch Audit Log, Data Quarantine)[cite: 1, 2].
  * Thực thi 100% tính toán (Joins, Deduplication, SCD Type 2 Point-in-Time, Aggregations) bằng Virtual Warehouse `EXPRESS_WH`[cite: 1, 2].

---

## 2. Cấu Trúc Thư Mục Dự Án

```text
express-delivery-dwh/
├── .env                                    # Biến môi trường kết nối Postgres & Snowflake
├── config/
│   ├── db_config.py                        # Quản lý kết nối SQLAlchemy (OLTP) & Snowflake Connector
│   └── settings.py                         # Đọc cấu hình môi trường & quy mô sinh dữ liệu
├── sql/
│   ├── oltp/
│   │   ├── 01_create_oltp_tables.sql       # DDL 11 bảng nguồn PostgreSQL OLTP
│   │   └── 02_seed_constraints.sql         # Index tối ưu truy vấn incremental theo updated_at
│   └── snowflake_init/
│       └── 01_init_snowflake.sql           # DDL khởi tạo Warehouse, DB, 4 Schemas, bảng META/STG & UDF
├── scripts/
│   ├── generate_oltp_data.py               # Sinh dữ liệu lần đầu (10,000 đơn) & mô phỏng Incremental + SCD2
│   ├── extract_load_snowflake.py           # Incremental Extract (REPEATABLE READ + Lookback 10p) -> Snowflake STG
│   ├── dq_gate_snowflake.py                # 11 quy tắc Data Quality Gate (Blocker & Warning) -> DATA_QUARANTINE
│   ├── post_load_watermark.py              # Cập nhật Watermark từ SOURCE_SNAPSHOT_TS & đóng Batch Log
│   └── run_local_pipeline.py               # Script chạy toàn bộ pipeline End-to-End
├── dbt_project/
│   ├── dbt_project.yml                     # Cấu hình dbt models theo các tầng Silver & Gold
│   ├── packages.yml                        # Khai báo package dbt_utils
│   ├── profiles.yml                        # Cấu hình kết nối tới Snowflake
│   ├── macros/
│   │   └── generate_schema_name.sql        # Macro giữ nguyên tên Schema chuẩn (SILVER, GOLD)
│   ├── snapshots/
│   │   ├── snap_customers.sql              # Theo dõi lịch sử SCD Type 2 cho Khách hàng
│   │   └── snap_couriers.sql               # Theo dõi lịch sử SCD Type 2 cho Bưu tá
│   └── models/
│       ├── sources.yml                     # Khai báo nguồn STG & META trên Snowflake
│       ├── schema.yml                      # Data tests kiểm tra PK, FK, accepted_values
│       ├── silver/                         # 11 bảng Silver (lọc Blocker, khử trùng lặp QUALIFY ROW_NUMBER)
│       └── gold/
│           ├── dimensions/                 # 8 bảng chiều (dim_date, dim_hub, dim_customer, dim_courier,...)
│           ├── facts/                      # 3 bảng sự kiện (fact_shipment_lifecycle, fact_tracking_events, fact_daily_hub_snapshot)
│           └── analytics/                  # 3 bảng/view báo cáo (v_slaship_summary, mv_daily_hub_performance, mv_courier_performance)
├── dags/
│   └── express_delivery_snowflake_dbt_dag.py # Airflow DAG điều phối tự động mỗi 4 giờ
└── docker/
    ├── docker-compose.yml                  # Định nghĩa container postgres-oltp & airflow
    ├── init-db.sql                         # Khởi tạo database express_oltp khi dựng container
    └── requirements.txt                    # Danh sách thư viện Python, Snowflake Connector & dbt
```

---

## 3. Bước 1: Chuẩn Bị Hạ Tầng Đám Mây (Snowflake Setup)

### 1.1. Đăng ký tài khoản Snowflake Free Trial

1. Truy cập [signup.snowflake.com](https://signup.snowflake.com/)[cite: 2].
2. Đăng ký gói **Enterprise Edition**, chọn Cloud Provider bất kỳ (khuyên dùng **AWS**), Region gần nhất (ví dụ: `ap-southeast-1` Singapore)[cite: 2].
3. Thiết lập **Username** và **Password** (lưu lại để cấu hình vào file `.env`)[cite: 2].

### 1.2. Lấy thông tin `SNOWFLAKE_ACCOUNT` (Account Identifier)

1. Trong giao diện Snowsight, bấm vào tên Profile ở góc dưới cùng bên trái $\rightarrow$ **Account** $\rightarrow$ di chuột vào biểu tượng Account[cite: 2].
2. Bấm **Copy Account Identifier** (hoặc Copy URL)[cite: 2].
3. Chuẩn hóa định dạng `SNOWFLAKE_ACCOUNT`[cite: 2]:
   * Nếu URL là `https://xy12345.ap-southeast-1.snowflakecomputing.com` $\rightarrow$ giá trị là `xy12345.ap-southeast-1`[cite: 2].
   * Nếu URL theo định dạng tổ chức `https://org-account.snowflakecomputing.com` $\rightarrow$ giá trị là `org-account` (ví dụ: `KCXSMSY-TH54040`)[cite: 2].
   * **Lưu ý:** Tuyệt đối không lấy tiền tố `https://` và hậu tố `.snowflakecomputing.com`[cite: 2].

### 1.3. Khởi tạo Cơ sở dữ liệu & Schemas trên Snowflake (Bắt buộc)

Trước khi kết nối từ máy local hoặc chạy Docker, bạn **phải khởi tạo sẵn Warehouse, Database `EXPRESS_DWH` và các bảng nền trên Snowflake**[cite: 1, 2]:

1. Đăng nhập vào [Snowflake Web Console](https://app.snowflake.com/), vào mục **Projects** $\rightarrow$ **Worksheets** $\rightarrow$ tạo một **SQL Worksheet** mới[cite: 2].
2. Mở file `sql/snowflake_init/01_init_snowflake.sql` trong thư mục dự án, sao chép toàn bộ nội dung và dán vào Worksheet trên Snowflake[cite: 1, 2].
3. Chọn **Select All** (Ctrl + A) và bấm **Run** (Ctrl + Enter) để tạo:
   * Virtual Warehouse: `EXPRESS_WH` (`XSMALL`, tự động ngủ sau 60s)[cite: 1].
   * Database: `EXPRESS_DWH` cùng 4 Schemas: `STG`, `SILVER`, `GOLD`, `META`[cite: 1].
   * 3 bảng quản trị ở tầng `META`: `ETL_WATERMARK`, `ETL_BATCH_LOG`, `DATA_QUARANTINE`[cite: 1].
   * 11 bảng thô ở tầng `STG` và hàm tiện ích `GOLD.FN_DATE_KEY`[cite: 1].

---

## 4. Bước 2: Cấu Hình Biến Môi Trường (`.env`)

Tại thư mục gốc của dự án (`express-delivery-dwh/`), tạo file `.env` với nội dung sau[cite: 2]:

```ini
# ==============================================================================
# 1. KẾT NỐI LOCAL POSTGRESQL (CONTAINER NGUỒN OLTP)
# ==============================================================================
# Lưu ý:
# - Khi chạy bằng Docker Compose: dùng host 'postgres-oltp'
# - Khi chạy script Python ngoài máy thật (host): đổi 'postgres-oltp' thành 'localhost'
OLTP_DB_URI=postgresql+psycopg2://postgres:postgres@postgres-oltp:5432/express_oltp

# ==============================================================================
# 2. KẾT NỐI CLOUD SNOWFLAKE (DATA WAREHOUSE)
# ==============================================================================
SNOWFLAKE_ACCOUNT=KCXSMSY-TH54040
SNOWFLAKE_USER=PHONGNHDATA25
SNOWFLAKE_PASSWORD=Your_Strong_Password_Here
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=EXPRESS_WH
SNOWFLAKE_DATABASE=EXPRESS_DWH
```

---

## 5. Bước 3: Thiết Lập Môi Trường Cục Bộ (Windows 64-bit) & Kiểm Tra Kết Nối

*(Bước này giúp cài đặt môi trường Python ảo trên máy thật để kiểm tra kết nối `dbt debug` tới Snowflake hoặc chạy script trực tiếp ngoài Docker)*[cite: 2].

### 3.1. Kiểm tra phiên bản Python 64-bit

Mở PowerShell và chạy lệnh[cite: 2]:

```powershell
python -c "import struct; print(struct.calcsize('P') * 8)"
```

Kết quả phải trả về `64`[cite: 2]. Nếu trả về `32`, hãy gỡ và cài đặt lại Python 64-bit từ trang chủ Python[cite: 2].

### 3.2. Tạo môi trường ảo (`venv`) và cài đặt thư viện

```powershell
# 1. Di chuyển vào thư mục gốc của dự án
cd express-delivery-dwh

# 2. Tạo virtual environment
python -m venv venv

# 3. Kích hoạt virtual environment
# (Nếu gặp lỗi ExecutionPolicy, chạy: Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process)
.\venv\Scripts\Activate.ps1

# 4. Nâng cấp pip và cài đặt đầy đủ thư viện
python -m pip install --upgrade pip setuptools wheel
pip install -r docker/requirements.txt
```

### 3.3. Nạp biến môi trường vào PowerShell, tải dbt packages và chạy `dbt debug`

```powershell
# 1. Nạp các biến từ file .env vào phiên PowerShell hiện tại
Get-Content .env | Where-Object { $_ -match '^[^#].*=.*' } | ForEach-Object {
    $parts = $_ -split '=', 2
    [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), "Process")
}

# 2. Di chuyển vào thư mục dbt_project
cd dbt_project

# 3. Tải package dbt-labs/dbt_utils
dbt deps --profiles-dir .

# 4. Kiểm tra kết nối từ dbt tới Snowflake Cloud
dbt debug --profiles-dir .
```

Nếu màn hình hiển thị thông báo[cite: 2]:

```text
All checks passed!
Connection test: [OK connection ok]
```

![Kết quả dbt deps và dbt debug](image/worklog/1794218352950.png)
![Chi tiết dbt debug thành công](image/worklog/1794218362622.png)

Nghĩa là `dbt-core` đã nhận diện thành công `profiles.yml` và kết nối thông suốt tới Database `EXPRESS_DWH` trên Snowflake[cite: 1, 2]. Quay lại thư mục gốc để chuẩn bị khởi chạy hệ thống[cite: 2]:

```powershell
cd ..
```

---

## 6. Bước 4: Khởi Động Và Vận Hành Hệ Thống

Bạn có thể lựa chọn **1 trong 2 cách** dưới đây để vận hành pipeline[cite: 2]:

### Cách 1: Khởi chạy toàn bộ bằng Docker Compose (Khuyên dùng)

Docker Compose sẽ tự động dựng container PostgreSQL OLTP (`express_postgres_oltp`), Apache Airflow (`express_airflow`), cài đặt các thư viện cần thiết, khởi tạo bảng OLTP, sinh dữ liệu mẫu và kích hoạt toàn bộ pipeline lần đầu tiên[cite: 1, 2].

```powershell
# 1. Đảm bảo đã tạo Database EXPRESS_DWH trên Snowflake (ở Bước 1.3)
# 2. Khởi động cụm dịch vụ dưới nền
docker compose -f docker/docker-compose.yml --env-file .env up -d

# 3. Theo dõi trực tiếp tiến trình chạy pipeline khởi tạo trong container Airflow
docker logs -f express_airflow
```

![Logs chạy thành công trên Docker](image/worklog/1794218770967.png)

Khi quá trình khởi tạo hoàn tất, bạn sẽ thấy thông báo trên màn hình log[cite: 1, 2]:

```text
===========================================================================
🎉 SNOWFLAKE + DBT PIPELINE HOÀN TẤT THÀNH CÔNG!
   • GOLD.FACT_SHIPMENT_LIFECYCLE : ... dòng
   • GOLD.FACT_TRACKING_EVENTS    : ... dòng
   • GOLD.FACT_DAILY_HUB_SNAPSHOT : ... dòng
   • META.DATA_QUARANTINE         : ... bản ghi lỗi/cảnh báo đã bắt được
===========================================================================
```

#### Quản lý & Lập lịch qua giao diện Airflow Web UI:

1. Mở trình duyệt truy cập: [http://localhost:8080](http://localhost:8080)[cite: 2]
2. Đăng nhập với tài khoản mặc định[cite: 1, 2]:
   * **Username**: `admin`[cite: 1, 2]
   * **Password**: `admin`[cite: 1, 2]
3. Tìm DAG có tên: `express_delivery_snowflake_elt`[cite: 1, 2].
4. Gạt công tắc **Unpause** sang màu xanh để Airflow tự động lập lịch chạy định kỳ mỗi 4 giờ (`0 */4 * * *`), hoặc bấm nút **Trigger DAG** (biểu tượng Play) để chạy thủ công[cite: 1, 2].

---

### Cách 2: Chạy trực tiếp bằng Python Script ngoài máy thật (Local Host)

Nếu chỉ muốn dựng container PostgreSQL làm nguồn OLTP và chạy trực tiếp script điều phối từ PowerShell trên máy thật[cite: 2]:

```powershell
# 1. Chỉ khởi động container PostgreSQL OLTP
docker compose -f docker/docker-compose.yml up -d postgres-oltp

# 2. Chuyển OLTP_DB_URI sang localhost cho phiên PowerShell hiện tại
$env:OLTP_DB_URI="postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"

# 3. Chạy toàn bộ pipeline (Lần 1: Full Seed 10,000 vận đơn -> Lần 2+: Incremental Batch 200 vận đơn + SCD2)
python scripts/run_local_pipeline.py
```

---

## 7. Bước 5: Luồng Xử Lý Chi Tiết Của Pipeline (7 Giai Đoạn)

Mỗi khi `run_local_pipeline.py` hoặc Airflow DAG `express_delivery_snowflake_elt` được kích hoạt, hệ thống thực thi tuần tự 7 bước nghiêm ngặt[cite: 1]:

1. **Khởi tạo & Sinh dữ liệu OLTP (`generate_oltp_data.py`)**:
   * **Lần chạy đầu tiên**: Tạo 1,500 khách hàng, 30 bưu cục (Hubs), 80 phương tiện, 200 bưu tá, 10,000 vận đơn cùng chuỗi sự kiện hành trình, thu hộ COD và khiếu nại (cố tình cài插 một số bản ghi lỗi `weight <= 0` và số điện thoại sai định dạng để kiểm thử Data Quality Gate)[cite: 1].
   * **Các lần chạy tiếp theo (Incremental)**: Tự động sinh thêm 200 vận đơn mới và cập nhật hạng thành viên của 15 khách hàng + đổi Hub/loại hợp đồng của 10 bưu tá để mô phỏng biến động **SCD Type 2**[cite: 1].
2. **Incremental Extract & Load (`extract_load_snowflake.py`)**:
   * Kiểm tra chống chạy chồng batch (`RUNNING`/`EXTRACTED` trong vòng 6 giờ)[cite: 1].
   * Mở transaction `REPEATABLE READ` đồng nhất cho cả 11 bảng trên PostgreSQL, kết hợp cửa sổ lùi 10 phút (`EXTRACT_LOOKBACK_MINUTES = 10`) dựa trên `META.ETL_WATERMARK` để không bỏ sót giao dịch commit muộn[cite: 1].
   * Nạp thẳng dữ liệu vào tầng `STG` trên Snowflake qua `write_pandas` và chỉ giữ lại tối đa 3 batch thành công gần nhất[cite: 1].
3. **Data Quality Gate & Quarantine (`dq_gate_snowflake.py`)**:
   * Quét 11 quy tắc chất lượng dữ liệu ngay trên tầng `STG`[cite: 1].
   * Cách ly toàn bộ bản ghi vi phạm lỗi nghiêm trọng (**Blocker**: `weight <= 0`, ngày giao thực tế nhỏ hơn ngày đặt, thiếu trường bắt buộc, timestamp ở tương lai...) và ghi nhận các cảnh báo (**Warning**: thiếu khóa ngoại phụ, số điện thoại sai định dạng, bản ghi con mồ côi) vào bảng `META.DATA_QUARANTINE` kèm bản sao JSON (`OBJECT_CONSTRUCT`)[cite: 1].
4. **Biến đổi Tầng Silver (`dbt run --select silver`)**:
   * Khử trùng lặp bằng `QUALIFY ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY updated_at DESC) = 1`[cite: 1].
   * Loại bỏ triệt để các bản ghi bị đánh dấu `Blocker` trong `META.DATA_QUARANTINE`, chuẩn hóa chuỗi ký tự, làm sạch số điện thoại và tính toán `row_hash` MD5[cite: 1].
5. **Ghi nhận Lịch sử SCD Type 2 (`dbt snapshot`)**:
   * Chạy `snap_customers` và `snap_couriers` theo chiến lược `check` trên cột `row_hash` từ tầng Silver để tự động đóng phiên bản cũ (`dbt_valid_to`) và mở phiên bản mới khi có thay đổi[cite: 1].
6. **Xây dựng Tầng Gold & Kiểm thử (`dbt run --select gold...` & `dbt test`)**:
   * Cập nhật 8 bảng **Dimensions** (đảm bảo mọi bảng đều có dòng `Unknown SK = -1` và tính khoảng hiệu lực `[valid_from, valid_to)` liền kề không khe hở)[cite: 1].
   * Nạp Incremental Merge cho 3 bảng **Facts** (`fact_shipment_lifecycle`, `fact_tracking_events`, `fact_daily_hub_snapshot`) với kỹ thuật Point-in-Time Join chính xác vào phiên bản SCD2 tại thời điểm phát sinh giao dịch[cite: 1].
   * Làm mới 3 Views/Tables phân tích (`v_slaship_summary`, `mv_daily_hub_performance`, `mv_courier_performance`) và chạy `dbt test` kiểm tra ràng buộc khóa[cite: 1].
7. **Cập nhật Watermark & Đóng Batch (`post_load_watermark.py`)**:
   * Chỉ khi tất cả các bước trên thành công 100%, hệ thống mới cập nhật `LAST_EXTRACTED_TIMESTAMP = SOURCE_SNAPSHOT_TS` vào `META.ETL_WATERMARK` và chuyển trạng thái batch sang `SUCCESS`[cite: 1].

---

## 8. Bước 6: Kiểm Tra & Đối Soát Dữ Liệu Trên Snowflake

Mở **Snowflake Web Console** $\rightarrow$ **Worksheets**, chọn Database `EXPRESS_DWH` và chạy bộ truy vấn dưới đây để kiểm chứng toàn bộ kiến trúc[cite: 1, 2]:

### 6.1. Thống kê số lượng bản ghi qua các tầng Medallion & Báo cáo nghiệp vụ

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE EXPRESS_WH;
USE DATABASE EXPRESS_DWH;

-- 1. Thống kê bản ghi qua từng tầng kiến trúc Medallion
SELECT '1. STG (Raw Load)' AS LAYER, COUNT(*) AS TOTAL_RECORDS FROM STG.SHIPMENTS
UNION ALL
SELECT '2. SILVER (Cleaned & Validated)', COUNT(*) FROM SILVER.SILVER_SHIPMENTS
UNION ALL
SELECT '3. GOLD (Fact Lifecycle Mart)', COUNT(*) FROM GOLD.FACT_SHIPMENT_LIFECYCLE
UNION ALL
SELECT '4. QUARANTINE (Lỗi Blocker bị cách ly)', COUNT(*) FROM META.DATA_QUARANTINE WHERE SEVERITY = 'Blocker';

-- 2. Báo cáo SLA giao hàng thực tế theo từng Bưu cục (Hub) xuất phát
SELECT 
    h.HUB_NAME,
    COUNT(f.SHIPMENT_SK) AS TOTAL_ORDERS,
    COUNT_IF(f.IS_ON_TIME) AS ON_TIME_ORDERS,
    COUNT_IF(f.IS_SLA_BREACHED) AS BREACHED_ORDERS,
    ROUND(100.0 * COUNT_IF(f.IS_ON_TIME) / NULLIF(COUNT_IF(f.IS_DELIVERED), 0), 2) AS OTD_PERCENTAGE
FROM GOLD.FACT_SHIPMENT_LIFECYCLE f
JOIN GOLD.DIM_HUB h ON f.ORIGIN_HUB_SK = h.HUB_SK
WHERE f.IS_DELIVERED = TRUE
GROUP BY h.HUB_NAME
ORDER BY TOTAL_ORDERS DESC;
```

### 6.2. Bộ 4 truy vấn kiểm toán tính toàn vẹn dữ liệu (Data Audit Queries)

```sql
USE DATABASE EXPRESS_DWH;

-- Audit 1: Đảm bảo không có bản ghi "mất im lặng" giữa STG và SILVER (Kỳ vọng = 0)
SELECT COUNT(*) AS MAT_IM_LANG
FROM STG.SHIPMENT_TRACKING_EVENTS e
WHERE NOT EXISTS (SELECT 1 FROM SILVER.SILVER_SHIPMENT_TRACKING_EVENTS s WHERE s.EVENT_ID = e.EVENT_ID)
  AND NOT EXISTS (
      SELECT 1 FROM META.DATA_QUARANTINE q
      WHERE q.SOURCE_TABLE = 'shipment_tracking_events'
        AND q.SEVERITY = 'Blocker'
        AND q.RECORD_ID = TO_VARCHAR(e.EVENT_ID)
  );

-- Audit 2: Trạng thái hiện tại trên Fact khớp 100% với cờ nghiệp vụ (Kỳ vọng = 0)
SELECT COUNT(*) AS LECH_TRANG_THAI
FROM GOLD.FACT_SHIPMENT_LIFECYCLE f
JOIN GOLD.DIM_STATUS_REASON sr ON sr.STATUS_REASON_SK = f.CURRENT_STATUS_SK
WHERE (f.IS_DELIVERED AND sr.STATUS_CODE <> 'DELIVERED')
   OR (f.IS_RETURNED  AND sr.STATUS_CODE <> 'RETURNED')
   OR (f.IS_CANCELLED AND sr.STATUS_CODE <> 'CANCELLED');

-- Audit 3: Mọi Fact trỏ đúng phiên bản SCD2 bao trùm thời điểm phát sinh (Kỳ vọng = 0, 0)
SELECT
    (SELECT COUNT(*) FROM GOLD.FACT_TRACKING_EVENTS e
     JOIN GOLD.DIM_COURIER c ON c.COURIER_SK = e.COURIER_SK
     WHERE NOT (e.EVENT_TIMESTAMP >= c.VALID_FROM AND e.EVENT_TIMESTAMP < c.VALID_TO)) AS SU_KIEN_SAI_PHIEN_BAN_SCD2,
    (SELECT COUNT(*) FROM GOLD.FACT_SHIPMENT_LIFECYCLE f
     JOIN GOLD.DIM_CUSTOMER c ON c.CUSTOMER_SK IN (f.SENDER_CUSTOMER_SK, f.RECEIVER_CUSTOMER_SK)
     WHERE NOT (f.ORDER_TIMESTAMP >= c.VALID_FROM AND f.ORDER_TIMESTAMP < c.VALID_TO)) AS VAN_DON_SAI_PHIEN_BAN_SCD2;

-- Audit 4: Đơn đã giao: Đúng hạn + Trễ hạn = Tổng đơn đã giao (Kỳ vọng 2 cột bằng nhau)
SELECT
    COUNT_IF(IS_DELIVERED) AS DELIVERED,
    COUNT_IF(IS_ON_TIME) + COUNT_IF(IS_SLA_BREACHED) AS ON_TIME_PLUS_BREACHED
FROM GOLD.FACT_SHIPMENT_LIFECYCLE;
```

---

## 9. Các Lệnh Quản Trị & Xử Lý Sự Cố Thường Dùng

| Thao tác                                                             | Câu lệnh Terminal / PowerShell                                                                                      |
| :-------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------- |
| **Khởi động toàn bộ hệ thống Docker**                    | `docker compose -f docker/docker-compose.yml --env-file .env up -d`[cite: 2]                                        |
| **Xem logs trực tiếp của container Airflow**                 | `docker logs -f express_airflow`[cite: 2]                                                                           |
| **Chạy thêm 1 batch Incremental + SCD2 ngay trong container** | `docker exec -it express_airflow python /opt/airflow/scripts/run_local_pipeline.py`[cite: 1, 2]                     |
| **Chạy riêng `dbt test` kiểm tra chất lượng dữ liệu** | `docker exec -it express_airflow bash -c "cd /opt/airflow/dbt_project && dbt test --profiles-dir ."`[cite: 1, 2]    |
| **Chạy `dbt compile` để xem mã SQL Snowflake thực tế**  | `docker exec -it express_airflow bash -c "cd /opt/airflow/dbt_project && dbt compile --profiles-dir ."`[cite: 1, 2] |
| **Dừng hệ thống container (giữ lại dữ liệu OLTP)**       | `docker compose -f docker/docker-compose.yml down`[cite: 2]                                                         |
| **Xóa sạch container & volume để làm lại từ đầu**      | `docker compose -f docker/docker-compose.yml down -v`[cite: 2]                                                      |

### Xử lý sự cố thường gặp (Troubleshooting):

* **Lỗi `Database 'EXPRESS_DWH' does not exist` khi mới bật Docker**: Do bạn chưa chạy script khởi tạo DDL trên Snowflake. Hãy thực hiện **Bước 1.3** (copy nội dung `sql/snowflake_init/01_init_snowflake.sql` chạy trên Snowflake Worksheet), sau đó chạy lại `docker restart express_airflow`[cite: 1, 2].
* **Lỗi `Batch ... đang chạy dở` (`[AUDIT-A5]`)**: Nếu một lần chạy trước bị ngắt đột ngột mà chưa kịp cập nhật trạng thái `FAILED`, hãy vào Snowflake chạy câu lệnh sau để mở khóa[cite: 1]:
  ```sql
  UPDATE EXPRESS_DWH.META.ETL_BATCH_LOG
  SET STATUS = 'FAILED', RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  WHERE STATUS IN ('RUNNING', 'EXTRACTED');
  ```
