# Express Delivery DWH: Kiến Trúc Lai (Hybrid Modern ELT)

**Local Orchestration (Docker, PostgreSQL, Airflow, dbt) $\longleftrightarrow$ Cloud Data Warehouse (Snowflake)**

---

## 1. Tổng quan Kiến Trúc Lai (Hybrid Architecture)

Hệ thống hoạt động theo mô hình **Hybrid Modern ELT**:

```text
[ LOCAL MACHINE / DOCKER ]                                     [ CLOUD (AWS / AZURE / GCP) ]
┌─────────────────────────┐                                   ┌──────────────────────────────────┐
│  PostgreSQL 15 (OLTP)   │                                   │       Snowflake Data Cloud       │
│  - Chứa DB nghiệp vụ     │                                  │                                  │
│  - Sinh dữ liệu giao dịch│                                  │  ┌────────────┐   ┌───────────┐  │
└────────────┬────────────┘                                   │  │ STG (Raw)  │──>│  SILVER   │  │
             │                                                │  └────────────┘   └─────┬─────┘  │
             ▼ (read_sql / write_pandas qua Internet)         │                         │        │
┌─────────────────────────┐                                   │                         ▼        │
│ Apache Airflow (Docker) │ ──[ Bắn lệnh dbt biên dịch ]────> │  ┌────────────┐   ┌───────────┐  │
│  - Điều phối lịch trình │                                   │  │ GOLD (Mart)│<──│ SCD2 Snap │  │
│  - Python EL Scripts    │                                   │  └────────────┘   └───────────┘  │
│  - dbt-core CLI runner  │                                   │  ┌───────────────────────────┐   │
└─────────────────────────┘                                   │  │ META (Watermark, Quarant) │   │
                                                              │  └───────────────────────────┘   │
                                                              │   (100% Compute & Storage Cloud) │
                                                              └──────────────────────────────────┘
```

### Phân định trách nhiệm tài nguyên:

* **Tại máy cục bộ (Local Container via Docker):**
  * Lưu trữ cơ sở dữ liệu nguồn `express_oltp` (PostgreSQL 15).
  * Chạy engine điều phối Apache Airflow (Scheduler, Webserver, MetaDB).
  * Cài đặt `dbt-core` và `dbt-snowflake`: đóng vai trò **client** nhận lệnh, biên dịch SQL/Jinja và gửi truy vấn qua API tới Snowflake. Máy local **không** tốn RAM/CPU để xử lý biến đổi bảng.
* **Tại hạ tầng đám mây (Snowflake Data Cloud):**
  * Lưu trữ 100% dữ liệu các tầng (`STG`, `SILVER`, `GOLD`, `META`).
  * Thực thi 100% tính toán (Transformations, Joins, Deduplication, SCD Type 2, Aggregations) thông qua Virtual Warehouse (`EXPRESS_WH`).

---

## 2. Chuẩn bị Hạ tầng Đám mây (Snowflake Setup)

### Bước 2.1: Đăng ký tài khoản Snowflake Free Trial

1. Truy cập [signup.snowflake.com](https://signup.snowflake.com/).
2. Đăng ký gói **Enterprise Edition**, chọn Cloud Provider bất kỳ (khuyên dùng **AWS**), Region gần nhất (ví dụ: `ap-southeast-1` Singapore).
3. Thiết lập thông tin đăng nhập:

   * **Username** (ví dụ: `EXPRESS_ADMIN`)
   * **Password** (lưu lại để đưa vào file cấu hình `.env`).

   ![1794215995865](image/worklog/1794215995865.png)

### Bước 2.2: Lấy thông tin `SNOWFLAKE_ACCOUNT` (Account Identifier)

1. Trong giao diện Snowsight, bấm vào tên Profile ở góc dưới cùng bên trái $\rightarrow$ **Account** $\rightarrow$ di chuột vào biểu tượng Account.
2. Bấm **Copy Account Identifier** (hoặc Copy URL).
3. Rút gọn định dạng chuẩn:
   * Nếu URL là `https://xy12345.ap-southeast-1.snowflakecomputing.com` $\rightarrow$ giá trị `SNOWFLAKE_ACCOUNT` là: `xy12345.ap-southeast-1`.
   * Nếu URL theo format tổ chức `https://org-account.snowflakecomputing.com` $\rightarrow$ giá trị là `org-account`.
   * *Tuyệt đối không lấy tiền tố `https://` và hậu tố `.snowflakecomputing.com`.*
   * ![1794216017904](image/worklog/1794216017904.png)

---

## 3. Cấu hình Môi trường Local (`.env`)

Tại thư mục gốc của project (`express-delivery-dwh/`), tạo file `.env` với nội dung sau:

```bash
# ==============================================================================
# 1. KẾT NỐI LOCAL POSTGRESQL (CONTAINER NGUỒN OLTP)
# ==============================================================================
# Lưu ý: Trong Docker network, Airflow kết nối tới Postgres qua service name 'postgres-oltp'
OLTP_DB_URI=postgresql+psycopg2://postgres:postgres@postgres-oltp:5432/express_oltp

# ==============================================================================
# 2. KẾT NỐI CLOUD SNOWFLAKE (DATA WAREHOUSE)
# ==============================================================================
SNOWFLAKE_ACCOUNT=KCXSMSY-TH54040
SNOWFLAKE_USER=PHONGNHDATA25
SNOWFLAKE_PASSWORD=Qewwqer132!#@Qewwqer132!#@
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=EXPRESS_WH
SNOWFLAKE_DATABASE=EXPRESS_DWH
```

---

## 4. Thiết Lập Môi Trường Cục Bộ Cho Máy Windows (64-bit)

### Bước 4.1: Kiểm tra phiên bản Python 64-bit

Mở PowerShell và kiểm tra:

```powershell
python -c "import struct; print(struct.calcsize('P') * 8)"
```

Kết quả phải in ra `64`. Nếu in ra `32`, hãy gỡ bỏ và cài đặt lại Python bản 64-bit từ trang chủ Python.

### Bước 4.2: Tạo môi trường ảo và cài đặt thư viện

```powershell
# Di chuyển vào thư mục dự án
cd express-delivery-dwh

# Tạo virtual environment
python -m venv venv

# Kích hoạt virtual environment
.\venv\Scripts\Activate.ps1
# Nếu bị lỗi Policy: Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

# Nâng cấp pip và cài đặt thư viện
python -m pip install --upgrade pip setuptools wheel
pip install -r docker/requirements.txt
```

### Bước 4.3: Tải Dependencies và Kiểm tra dbt với Snowflake

Di chuyển vào thư mục `dbt_project`:

```powershell
cd dbt_project

# Tải package dbt_utils
dbt deps

# Kiểm tra kết nối từ dbt đến Snowflake Cloud
dbt debug
```

Nếu màn hình thông báo:

```text
All checks passed!
Connection test: [OK connection ok]
```

![1794218352950](image/worklog/1794218352950.png)![1794218362622](image/worklog/1794218362622.png)

Nghĩa là `dbt-core` đã nhận diện cấu hình trong `profiles.yml` và kết nối thông suốt tới Snowflake. Hãy quay lại thư mục gốc:

```powershell
cd ..
```

---

## 5. Khởi Động Và Vận Hành Hệ Thống

### Cách 1: Khởi chạy toàn bộ bằng Docker Compose (Khuyên dùng)

Hệ thống sẽ dựng toàn bộ: PostgreSQL OLTP, Apache Airflow Scheduler & Webserver, đồng thời tự động kích hoạt dbt bên trong container.

```powershell
# Khởi động cụm dịch vụ
docker compose -f docker/docker-compose.yml --env-file .env up -d

# Theo dõi quá trình khởi tạo tự động
docker logs -f express_airflow
```

![1794218770967](image/worklog/1794218770967.png)

Khi container hoàn thành việc khởi tạo cơ sở dữ liệu và kích hoạt pipeline, bạn sẽ thấy thông báo:
`🎉 SNOWFLAKE + DBT PIPELINE HOÀN TẤT THÀNH CÔNG!`



----- chỗ này phải vào snowflake tạo csdl data warehouse trước, ccopy lênh trong file "express-delivery-dwh\sql\snowflake_init\01_init_snowflake.sql" r tạo csdl

#### Quản lý qua giao diện Airflow Web UI:

1. Mở trình duyệt: [http://localhost:8080](http://localhost:8080)
2. Đăng nhập:
   - **Username**: `admin`
   - **Password**: `admin`
3. Tìm DAG có tên: `express_delivery_snowflake_elt`.
4. Bật công tắc **Unpause** sang màu xanh để Airflow tự động lập lịch chạy mỗi 4 tiếng.

---

### Cách 2: Chạy trực tiếp qua Python script ngoài máy thật

Nếu bạn đã bật container PostgreSQL (`docker run` hoặc dùng container `postgres-oltp`):

```powershell
# Chạy toàn bộ pipeline ETL + Data Quality + dbt Models
python scripts/run_local_pipeline.py
```

---

## 6. Kiểm Tra & Đối Soát Dữ Liệu Trên Snowflake

Sau khi chạy xong, hãy đăng nhập vào [Snowflake Web Console](https://app.snowflake.com/), mở mục **Worksheets** $\rightarrow$ tạo một Worksheet SQL mới và chạy các câu truy vấn xác nhận:

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
SELECT '4. QUARANTINE (Lỗi bị cách ly)', COUNT(*) FROM META.DATA_QUARANTINE WHERE SEVERITY = 'Blocker';

-- 2. Kiểm tra tính toàn vẹn của lịch sử SCD Type 2
-- Kỳ vọng: Không có sự kiện nào rơi vào khoảng thời gian sai phiên bản (Kết quả = 0)
SELECT COUNT(*) AS INVALID_SCD2_EVENTS
FROM GOLD.FACT_TRACKING_EVENTS e
JOIN GOLD.DIM_COURIER c ON c.COURIER_SK = e.COURIER_SK
WHERE NOT (e.EVENT_TIMESTAMP >= c.VALID_FROM AND e.EVENT_TIMESTAMP < c.VALID_TO);

-- 3. Báo cáo SLA giao hàng thực tế theo từng loại Hub xuất phát
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

---

## 7. Các Lệnh Quản Trị Thường Dùng

| Thao tác                                                         | Câu lệnh Terminal / PowerShell                                                         |
| :---------------------------------------------------------------- | :--------------------------------------------------------------------------------------- |
| **Xem logs container Airflow**                              | `docker logs -f express_airflow`                                                       |
| **Dừng hệ thống container**                              | `docker compose -f docker/docker-compose.yml down`                                     |
| **Xóa sạch container & volume làm lại từ đầu**       | `docker compose -f docker/docker-compose.yml down -v`                                  |
| **Chạy riêng test dbt kiểm tra chất lượng dữ liệu** | `docker exec -it express_airflow bash -c "cd /opt/airflow/dbt_project && dbt test"`    |
| **Chạy dbt compile xem mã SQL Snowflake thực tế**       | `docker exec -it express_airflow bash -c "cd /opt/airflow/dbt_project && dbt compile"` |
