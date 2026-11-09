# 🚚 Express Delivery Data Warehouse (PostgreSQL OLTP $\rightarrow$ Snowflake + dbt + Airflow)

Dự án xây dựng Kho dữ liệu (Data Warehouse) theo kiến trúc **Modern Data Stack (ELT)** kết hợp **Medallion Architecture (`STG` $\rightarrow$ `SILVER` $\rightarrow$ `GOLD`)** và **Kimball Dimensional Modeling (Star Schema + SCD Type 2)** cho bài toán Giao hàng Nhanh (Express Delivery).

---

## 📐 Kiến trúc Tổng quan

1. **Source OLTP (`PostgreSQL 15`)**: Giả lập hệ thống giao dịch thực tế gồm 11 bảng nghiệp vụ (Vận đơn, Sự kiện hành trình, Khách hàng, Bưu tá, Bưu cục, Phương tiện, Đối soát COD, Khiếu nại...).
2. **Extract & Load (`Python + Snowflake Connector`)**: Đọc dữ liệu gia tăng (Incremental Extract) theo cơ chế Watermark kết hợp cửa sổ lùi (`Lookback Window 10 phút`) trong một Snapshot `REPEATABLE READ` đồng nhất và đẩy thẳng lên tầng `STG` của Snowflake.
3. **Data Quality Gate (`META.DATA_QUARANTINE`)**: Chặn lọc tự động 11 quy tắc chất lượng dữ liệu:
   - **Blocker**: Cách ly hoàn toàn các bản ghi vi phạm nghiêm trọng (trọng lượng $\le 0$, ngày giao trước ngày đặt, thiếu trường bắt buộc...) không cho vào `SILVER`.
   - **Warning**: Ghi nhận cảnh báo và tự động xử lý làm sạch hoặc ánh xạ về khóa ngoại mặc định (`Unknown SK = -1`).
4. **Transformation (`dbt Core + dbt-snowflake`)**:
   - **Tầng `SILVER`**: Làm sạch, chuẩn hóa định dạng, khử trùng lặp (`QUALIFY ROW_NUMBER() = 1`) và hợp nhất (`MERGE`).
   - **Snapshots (`SCD Type 2`)**: Theo dõi lịch sử thay đổi hạng thành viên khách hàng (`snap_customers`) và lịch sử điều chuyển bưu cục/hợp đồng của bưu tá (`snap_couriers`).
   - **Tầng `GOLD`**: Xây dựng 8 bảng Dimension, 3 bảng Fact (Transaction, Accumulating Snapshot, Periodic Daily Snapshot) và 3 bảng/view Semantic phục vụ BI.
5. **Orchestration (`Apache Airflow 2.9`)**: Tự động hóa toàn bộ quy trình theo lịch định kỳ.

---

## 🛠️ BƯỚC 1: Đăng ký tài khoản Snowflake & Lấy thông tin kết nối

Nếu bạn chưa có tài khoản Snowflake, bạn có thể đăng ký gói dùng thử miễn phí **30 ngày ($400 credit)** không cần thẻ tín dụng:

### 1.1. Đăng ký tài khoản

1. Truy cập trang đăng ký: [https://signup.snowflake.com/](https://signup.snowflake.com/)
2. Điền thông tin cá nhân (Họ tên, Email, Công ty/Trường học, Quốc gia: **Vietnam**).
3. Ở bước chọn phiên bản:
   - **Snowflake Edition**: Chọn **Enterprise** (để hỗ trợ đầy đủ tính năng nâng cao).
   - **Cloud Provider**: Chọn **AWS (Amazon Web Services)**.
   - **Region**: Chọn **Asia Pacific (Singapore - ap-southeast-1)** để có tốc độ kết nối nhanh nhất từ Việt Nam.
4. Nhấn **Get Started**. Mở hộp thư Email của bạn, tìm email kích hoạt từ Snowflake và nhấn **Click to Activate**.
5. Đặt **Username** (ví dụ: `express_admin`) và **Password** đăng nhập.

### 1.2. Cách lấy đúng mã `SNOWFLAKE_ACCOUNT` (Cực kỳ quan trọng)

Sau khi đăng nhập vào giao diện Snowflake (Snowsight):

1. Nhìn xuống **góc dưới cùng bên trái** màn hình, bấm vào tên tài khoản của bạn.
2. Di chuột vào mục **Account** $\rightarrow$ bấm vào biểu tượng **Copy account identifier** (hoặc **View account details**).
3. Chuyển dấu chấm (`.`) giữa tên Organization và tên Account thành dấu gạch ngang (`-`), hoặc dùng mã `locator.region.cloud`:
   - **Cách 1 (Khuyên dùng - `<orgname>-<account_name>`)**: Ví dụ nếu bạn copy được `ABCD123.XY98765`, hãy đổi thành `ABCD123-XY98765`.
   - **Cách 2 (Theo URL trình duyệt)**: Nếu URL đăng nhập của bạn là `https://xy12345.ap-southeast-1.aws.snowflakecomputing.com`, thì `SNOWFLAKE_ACCOUNT` của bạn chính là **`xy12345.ap-southeast-1.aws`**.

> 💡 **Lưu ý về quyền (Role)**: Tài khoản vừa tạo mặc định có quyền `ACCOUNTADMIN`. Hệ thống sẽ dùng quyền này để tự động tạo Warehouse `EXPRESS_WH` và Database `EXPRESS_DWH`.

---

## 📂 BƯỚC 2: Chuẩn bị Cấu trúc Thư mục Dự án

Đảm bảo bạn đã lưu đầy đủ các file từ tài liệu thiết kế vào đúng cấu trúc thư mục sau:

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
│       ├── silver/        (11 file .sql)
│       └── gold/
│           ├── dimensions/ (8 file .sql)
│           ├── facts/      (3 file .sql)
│           └── analytics/  (3 file .sql)
├── dags/
│   └── express_delivery_snowflake_dbt_dag.py
└── docker/
    ├── docker-compose.yml
    ├── init-db.sql
    └── requirements.txt
```

---

## ⚙️ BƯỚC 3: Cấu hình Biến Môi trường (`.env`)

Tạo một file có tên `.env` nằm bên trong thư mục **`docker/.env`** (cùng cấp với file `docker-compose.yml`) với nội dung sau:

```ini
# Thông tin tài khoản Snowflake của bạn (Lấy ở Bước 1)
SNOWFLAKE_ACCOUNT= abc12345.ap-southeast-1.aws
SNOWFLAKE_USER=express_admin
SNOWFLAKE_PASSWORD=MatKhauSnowflakeCuaBan123!
SNOWFLAKE_ROLE=ACCOUNTADMIN
SNOWFLAKE_WAREHOUSE=EXPRESS_WH
SNOWFLAKE_DATABASE=EXPRESS_DWH
```

*(Thay thế `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, và `SNOWFLAKE_PASSWORD` bằng thông tin thực tế của bạn).*

---

## 🚀 BƯỚC 4: Khởi chạy Hệ thống (Chọn 1 trong 2 cách)

Bạn có thể chạy toàn bộ hệ thống tự động hoàn toàn qua **Docker Compose** (Cách 1 - Khuyên dùng) hoặc chạy trực tiếp bằng **Python trên máy cá nhân** kết hợp container PostgreSQL (Cách 2 - Thuận tiện khi cần debug từng câu lệnh dbt).

---

### CÁCH 1: Chạy trọn gói bằng Docker Compose (PostgreSQL + Airflow + dbt)

**Yêu cầu:** Máy tính đã cài đặt và bật sẵn **Docker Desktop**.

#### 1. Khởi động các container

Mở Terminal / PowerShell, di chuyển vào thư mục `docker/` và chạy lệnh:

```bash
cd express-delivery-dwh/docker
docker compose up -d
```

#### 2. Theo dõi tiến trình khởi tạo tự động (Batch 1 - Full Load)

Ngay khi container `express_airflow` khởi động, nó sẽ tự động cài đặt các thư viện, tạo bảng OLTP, sinh **10.000 vận đơn mẫu**, tạo Warehouse/Database trên Snowflake và chạy trọn vẹn **Batch 1 (Full Load)**. Bạn có thể xem trực tiếp log bằng lệnh:

```bash
docker logs -f express_airflow
```

Đợi khoảng 2–3 phút cho đến khi màn hình xuất hiện thông báo:

```text
🎉 SNOWFLAKE + DBT PIPELINE HOÀN TẤT THÀNH CÔNG!
   • GOLD.FACT_SHIPMENT_LIFECYCLE : 9,960 dòng
   • GOLD.FACT_TRACKING_EVENTS    : 41,800+ dòng
   • GOLD.FACT_DAILY_HUB_SNAPSHOT : 840+ dòng
   • META.DATA_QUARANTINE         : 190+ bản ghi lỗi/cảnh báo đã bắt được
```

#### 3. Truy cập giao diện Apache Airflow & Chạy Batch 2 (Incremental + SCD2)

1. Mở trình duyệt truy cập: [http://localhost:8080](http://localhost:8080)
2. Đăng nhập với tài khoản:

   - **Username**: `admin`
   - **Password**: `admin`
3. Để mô phỏng phát sinh dữ liệu mới ở OLTP (thêm **200 vận đơn mới** + đổi hạng thành viên của 15 khách hàng + điều chuyển bưu cục của 10 bưu tá để kiểm thử **SCD Type 2**), bạn có thể chạy lệnh sau trong Terminal:

   ```bash
   docker exec -it express_airflow python /opt/airflow/scripts/run_local_pipeline.py
   ```

   Hoặc chỉ sinh dữ liệu mới ở OLTP rồi bấm nút **Trigger DAG (`▶️`)** trên giao diện Airflow cho DAG `express_delivery_snowflake_elt`:
   ```bash
   # Chỉ sinh thêm dữ liệu Incremental ở Postgres OLTP
   docker exec -it express_airflow python -c "from scripts.generate_oltp_data import generate_data; generate_data()"
   ```

   Sau đó vào Airflow Web UI $\rightarrow$ Bật công tắc DAG `express_delivery_snowflake_elt` sang **ON** $\rightarrow$ Nhấn nút **Play (`▶️`)** để xem từng task chạy xanh lần lượt từ `preflight_checks` $\rightarrow$ `dbt_test_marts` $\rightarrow$ `update_watermark_and_audit_log`.

---

### CÁCH 2: Chạy trực tiếp trên máy Local (Dành cho Developer muốn chạy lẻ từng bước)

Nếu bạn muốn chạy trực tiếp lệnh `python` và `dbt` trên máy tính (không cần bật Airflow):

#### 1. Chỉ bật PostgreSQL OLTP bằng Docker

```bash
cd express-delivery-dwh/docker
docker compose up -d postgres-oltp
```

#### 2. Tạo môi trường ảo Python & Cài đặt thư viện

Quay về thư mục gốc của dự án (`express-delivery-dwh/`):

```bash
cd ..
python -m venv .venv

# Kích hoạt môi trường ảo:
# - Trên Windows (PowerShell):
.venv\Scripts\Activate.ps1
# - Trên macOS / Linux:
source .venv/bin/activate

# Cài đặt thư viện
pip install -r docker/requirements.txt
```

#### 3. Nạp biến môi trường trên Terminal

- **Trên Windows (PowerShell)**:
  ```powershell
  $env:OLTP_DB_URI="postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
  $env:SNOWFLAKE_ACCOUNT="abc12345.ap-southeast-1.aws"
  $env:SNOWFLAKE_USER="express_admin"
  $env:SNOWFLAKE_PASSWORD="MatKhauSnowflakeCuaBan123!"
  $env:SNOWFLAKE_ROLE="ACCOUNTADMIN"
  $env:SNOWFLAKE_WAREHOUSE="EXPRESS_WH"
  $env:SNOWFLAKE_DATABASE="EXPRESS_DWH"
  ```
- **Trên macOS / Linux (Bash/Zsh)**:
  ```bash
  export OLTP_DB_URI="postgresql+psycopg2://postgres:postgres@localhost:5432/express_oltp"
  export SNOWFLAKE_ACCOUNT="abc12345.ap-southeast-1.aws"
  export SNOWFLAKE_USER="express_admin"
  export SNOWFLAKE_PASSWORD="MatKhauSnowflakeCuaBan123!"
  export SNOWFLAKE_ROLE="ACCOUNTADMIN"
  export SNOWFLAKE_WAREHOUSE="EXPRESS_WH"
  export SNOWFLAKE_DATABASE="EXPRESS_DWH"
  ```

#### 4. Chạy Pipeline Lần 1 (Khởi tạo DDL + Seed 10.000 đơn + Full Load)

```bash
python scripts/run_local_pipeline.py
```

#### 5. Chạy Pipeline Lần 2 (Mô phỏng Incremental + Cập nhật SCD Type 2)

Chạy lại đúng câu lệnh trên một lần nữa:

```bash
python scripts/run_local_pipeline.py
```

Ở lần chạy thứ 2, script sẽ phát hiện bảng `shipments` đã có dữ liệu và tự động chuyển sang chế độ **Incremental Batch**:

- Cập nhật hạng thẻ của 15 khách hàng và điều chuyển bưu cục của 10 bưu tá.
- Sinh thêm 200 vận đơn mới.
- Chỉ trích xuất các bản ghi thay đổi sau mốc Watermark và tạo phiên bản lịch sử mới (`SCD Type 2`) trong `GOLD.DIM_CUSTOMER` và `GOLD.DIM_COURIER`.

---

## 🔍 BƯỚC 5: Kiểm tra & Đối soát kết quả trên Snowflake (Snowsight)

Đăng nhập vào giao diện Snowflake, mở một **SQL Worksheet** mới và chạy các câu truy vấn dưới đây để kiểm nghiệm toàn bộ kiến trúc:

### 5.1. Kiểm tra lịch sử chạy Batch & Watermark (`META`)

```sql
USE DATABASE EXPRESS_DWH;

-- 1. Xem mốc Watermark hiện tại của 11 bảng
SELECT * FROM META.ETL_WATERMARK ORDER BY TABLE_NAME;

-- 2. Xem nhật ký từng Batch (số dòng trích xuất, bị từ chối Blocker và nạp thành công)
SELECT BATCH_ID, TABLE_NAME, ROWS_EXTRACTED, ROWS_REJECTED, ROWS_LOADED, STATUS, SOURCE_SNAPSHOT_TS
FROM META.ETL_BATCH_LOG
ORDER BY RUN_START DESC, TABLE_NAME;

-- 3. Xem thống kê các lỗi dữ liệu bị bắt giữ tại chốt chặn Data Quality Gate
SELECT RULE_NAME, SEVERITY, SOURCE_TABLE, COUNT(*) AS TOTAL_RECORDS
FROM META.DATA_QUARANTINE
GROUP BY RULE_NAME, SEVERITY, SOURCE_TABLE
ORDER BY SEVERITY, TOTAL_RECORDS DESC;
```

### 5.2. Kiểm tra lịch sử biến đổi SCD Type 2 (Sau khi chạy từ 2 Batch trở lên)

```sql
-- Xem các khách hàng có nhiều hơn 1 phiên bản lịch sử (đổi hạng thành viên)
SELECT CUSTOMER_SK, CUSTOMER_NK, CUSTOMER_NAME, MEMBERSHIP_TIER, VALID_FROM, VALID_TO, IS_CURRENT
FROM GOLD.DIM_CUSTOMER
WHERE CUSTOMER_NK IN (
    SELECT CUSTOMER_NK FROM GOLD.DIM_CUSTOMER GROUP BY CUSTOMER_NK HAVING COUNT(*) > 1
)
ORDER BY CUSTOMER_NK, VALID_FROM;

-- Xem các bưu tá được điều chuyển bưu cục hoặc thay đổi loại hợp đồng
SELECT COURIER_SK, COURIER_NK, COURIER_NAME, ASSIGNED_HUB_CODE, CONTRACT_TYPE, VALID_FROM, VALID_TO, IS_CURRENT
FROM GOLD.DIM_COURIER
WHERE COURIER_NK IN (
    SELECT COURIER_NK FROM GOLD.DIM_COURIER GROUP BY COURIER_NK HAVING COUNT(*) > 1
)
ORDER BY COURIER_NK, VALID_FROM;
```

### 5.3. Bốn câu truy vấn Đối soát Chất lượng (Audit Queries)

```sql
-- 1. Kiểm tra không có bản ghi "mất im lặng" giữa STG và SILVER (Kỳ vọng = 0)
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

### 5.4. Truy vấn Báo cáo Phân tích (Tầng Semantic / Analytics)

```sql
-- 1. Báo cáo tỷ lệ giao hàng đúng hạn (OTD %) và doanh thu theo loại dịch vụ
SELECT
    SERVICE_NAME,
    SUM(TOTAL_SHIPMENTS) AS TOTAL_ORDERS,
    SUM(DELIVERED_SHIPMENTS) AS DELIVERED_ORDERS,
    ROUND(100.0 * SUM(ON_TIME_SHIPMENTS) / NULLIF(SUM(DELIVERED_SHIPMENTS), 0), 2) AS OTD_PCT,
    SUM(TOTAL_FREIGHT_REVENUE) AS TOTAL_REVENUE
FROM GOLD.V_SLASHIP_SUMMARY
GROUP BY SERVICE_NAME
ORDER BY TOTAL_ORDERS DESC;

-- 2. Hiệu suất vận hành & tỷ lệ tồn kho của các Bưu cục (Hub Performance)
SELECT * FROM GOLD.MV_DAILY_HUB_PERFORMANCE
ORDER BY SNAPSHOT_DATE DESC, BACKLOG_CAPACITY_RATIO_PCT DESC
LIMIT 20;

-- 3. Hiệu suất giao hàng của Bưu tá theo từng phiên bản lịch sử công tác (Courier SCD2)
SELECT * FROM GOLD.MV_COURIER_PERFORMANCE
ORDER BY YEAR DESC, MONTH DESC, DELIVERED_SHIPMENTS DESC
LIMIT 20;
```

---

## 🛠️ BƯỚC 6: Xử lý Sự cố Thường gặp (Troubleshooting)

1. **Lỗi `250001 (08001): Failed to connect to DB: ... incorrect username or password` hoặc `404 Not Found`:**
   - Nguyên nhân: Sai định dạng `SNOWFLAKE_ACCOUNT`.
   - Cách khắc phục: Không bao gồm `https://` hoặc `.snowflakecomputing.com` trong `SNOWFLAKE_ACCOUNT`. Chỉ điền `<org_name>-<account_name>` (ví dụ: `ABCD123-XY98765`) hoặc `<locator>.<region>.<cloud>` (ví dụ: `xy98765.ap-southeast-1.aws`).
2. **Lỗi `Batch BATCH_XXXXX đang chạy dở`:**
   - Nguyên nhân: Lần chạy trước bị ngắt đột ngột (ví dụ tắt nóng cửa sổ Terminal) khi trạng thái trong `META.ETL_BATCH_LOG` vẫn đang là `RUNNING` hoặc `EXTRACTED`.
   - Cách khắc phục: Mở Snowflake Worksheet và chạy lệnh sau để đóng batch bị treo:
     ```sql
     UPDATE EXPRESS_DWH.META.ETL_BATCH_LOG
     SET STATUS = 'FAILED', RUN_END = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
     WHERE STATUS IN ('RUNNING', 'EXTRACTED');
     ```
3. **Muốn xóa sạch toàn bộ dữ liệu để chạy lại từ đầu (Reset 100%):**
   - Trên Snowflake Worksheet:
     ```sql
     DROP DATABASE IF EXISTS EXPRESS_DWH;
     ```
   - Dưới thư mục `docker/`:
     ```bash
     docker compose down -v
     docker compose up -d
     ```
