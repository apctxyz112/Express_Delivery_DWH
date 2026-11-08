
# EXPRESS DELIVERY DATA WAREHOUSE

## Master Implementation Blueprint

**Phiên bản tài liệu:** 1.0
**Mục tiêu:** Xây dựng Data Warehouse cho ngành Chuyển phát nhanh theo định hướng enterprise, chạy local trước, sau đó đóng gói bằng Airflow Docker.
**Mô hình áp dụng:** Medallion Architecture + Kimball Dimensional Modeling
**Quy mô dữ liệu:** khoảng 10,000 shipments, 40,000–50,000 tracking events.

---

## 1. Mục tiêu của đồ án

Dự án này hướng đến việc xây dựng một hệ thống Data Warehouse hoàn chỉnh cho doanh nghiệp chuyển phát nhanh, bao gồm:

1. Giả lập hệ thống OLTP nguồn.
2. Thiết kế Data Warehouse theo 3 tầng:
   - Bronze / Staging
   - Silver / Cleansed & Conformed
   - Gold / Dimensional Data Store
3. Xây dựng pipeline ETL:
   - Incremental load
   - Data quality gate
   - SCD Type 2
   - Fact loading theo Kimball
4. Tối ưu hiệu năng truy vấn.
5. Xây dựng dashboard báo cáo.
6. Đóng gói pipeline bằng Airflow Docker.

### Tiêu chí thành công

- Pipeline chạy được end-to-end trên local.
- Dữ liệu được nạp incremental, không full load tùy tiện.
- Có kiểm soát chất lượng dữ liệu.
- Có audit log, watermark, batch log.
- Mô hình Gold đúng tư duy Kimball.
- Dashboard trả lời được câu hỏi nghiệp vụ.
- Có thể đóng gói lại bằng Airflow.

---

## 2. Phạm vi triển khai

### Bắt buộc

- OLTP source với các bảng chính.
- Data Warehouse 3 tầng.
- 3 bảng Fact:
  - `Fact_Shipment_Lifecycle`
  - `Fact_Tracking_Events`
  - `Fact_Daily_Hub_Snapshot`
- 8 Dimensions:
  - `Dim_Date`
  - `Dim_Customer`
  - `Dim_Courier`
  - `Dim_Hub`
  - `Dim_Service_Type`
  - `Dim_Vehicle`
  - `Dim_Status_Reason`
  - `Dim_Payment_Method`
- ETL:
  - Incremental extract
  - Data quality gate
  - SCD Type 2 cho Customer/Courier
  - Merge/Upsert cho Accumulating Fact
- Dashboard:
  - SLA / OTD
  - Hub bottleneck
  - Courier performance
- Airflow DAG đóng gói pipeline.

### Mở rộng nếu còn thời gian

- Fact COD Settlement.
- Dim_Route.
- Materialized views nâng cao.
- Data tests tự động.
- Partitioning.
- Benchmark execution plan.

---

## 3. Stack khuyến nghị

### Phương án khuyến nghị cho đồ án

| Thành phần  | Lựa chọn ưu tiên           | Lý do                                  |
| ------------- | ------------------------------ | --------------------------------------- |
| Database OLTP | PostgreSQL                     | Dễ chạy local, dễ dùng với Airflow |
| Database DWH  | PostgreSQL                     | Đồng bộ stack, nhẹ, dễ debug       |
| ETL           | Python                         | Linh hoạt, dễ viết pipeline          |
| Thư viện    | SQLAlchemy / pandas / Faker    | Dễ sinh dữ liệu và thao tác DB     |
| Orchestration | Apache Airflow                 | Chuẩn enterprise                       |
| Container     | Docker Compose                 | Dễ đóng gói                         |
| BI            | Power BI hoặc Apache Superset | Power BI dễ demo hơn                  |

### Nếu dùng SQL Server

Phù hợp nếu bạn quen với:

- `MERGE`
- SQL Server Management Studio
- Columnstore index

Nhưng cần lưu ý:

- SQL Server container nặng hơn.
- Airflow vẫn thường dùng PostgreSQL làm metadata DB.
- Cần kiểm tra phiên bản SQL Server có hỗ trợ đầy đủ tính năng bạn muốn demo hay không.

---

## 4. Kiến trúc tổng thể

## 4.1. Sơ đồ kiến trúc Medallion

```mermaid
flowchart LR
    A[OLTP Source] --> B[Bronze / Staging]
    B --> C[Silver / Cleansed]
    C --> D[Gold / Dimensional DWH]
    D --> E[Semantic Layer / Views]
    E --> F[Power BI / Superset]

    B --> Q[Quarantine]
    C --> Q
    D --> M[Meta / Audit]
    M --> B
    M --> C
    M --> D
```

### Diễn giải

- **OLTP Source:** dữ liệu nghiệp vụ gốc của hệ thống chuyển phát nhanh.
- **Bronze / Staging:** dữ liệu thô được extract theo batch.
- **Silver / Cleansed:** dữ liệu đã chuẩn hóa, kiểm tra chất lượng.
- **Gold / DDS:** dữ liệu dimensional phục vụ phân tích.
- **Semantic Layer:** views / materialized views / data marts.
- **Meta / Audit:** watermark, batch log, quarantine.

---

## 4.2. Sơ đồ luồng dữ liệu tổng thể

```mermaid
flowchart TD
    S[OLTP Database] --> E[Incremental Extract]
    E --> STG[Staging Schema]
    STG --> DQ[Data Quality Gate]
    DQ -->|Valid| SIL[Silver Schema]
    DQ -->|Invalid| QUAR[Quarantine Table]
    SIL --> DIM[Load Dimensions]
    SIL --> FACT[Load Facts]
    DIM --> GOLD[Gold Schema]
    FACT --> GOLD
    GOLD --> VIEW[Analytical Views]
    VIEW --> BI[Dashboard]
    E --> META[ETL Control Log]
    DQ --> META
    DIM --> META
    FACT --> META
```

---

## 5. Thiết kế tầng OLTP nguồn

## 5.1. Danh sách bảng OLTP cốt lõi

| Bảng                        | Ý nghĩa                          | Số lượng dự kiến |
| ---------------------------- | ---------------------------------- | --------------------: |
| `customers`                | Khách gửi / khách nhận         |        1,000 – 2,000 |
| `hubs`                     | Bưu cục / trung tâm phân loại |              20 – 50 |
| `couriers`                 | Shipper                            |            100 – 300 |
| `vehicles`                 | Phương tiện                     |             50 – 100 |
| `service_types`            | Loại dịch vụ                    |               5 – 10 |
| `shipments`                | Vận đơn                         |                10,000 |
| `shipment_tracking_events` | Sự kiện quét / hành trình     |      40,000 – 50,000 |
| `customer_tickets`         | Khiếu nại                        |          300 – 1,000 |
| `cod_settlements`          | Đối soát COD                    |            tùy chọn |

---

## 5.2. Nguyên tắc thiết kế OLTP

Tất cả bảng nguồn cần có:

- `created_at`
- `updated_at`

Các bảng nghiệp vụ chính nên có:

- natural key
- trạng thái chuẩn
- tham chiếu FK hợp lệ
- dữ liệu thời gian hợp logic

---

## 5.3. Ví dụ các trường quan trọng

### `customers`

```text
customer_id
customer_code
customer_name
customer_type      -- Individual / Shop / B2B
membership_tier    -- Bronze / Silver / Gold
phone_number
city
district
created_at
updated_at
```

### `hubs`

```text
hub_id
hub_code
hub_name
hub_level          -- Regional Hub / Local Branch
city
district
capacity
created_at
updated_at
```

### `couriers`

```text
courier_id
courier_code
courier_name
hub_id
contract_type      -- Full-time / Part-time
vehicle_id
effective_date
created_at
updated_at
```

### `shipments`

```text
shipment_id
shipment_number
sender_customer_id
receiver_customer_id
origin_hub_id
destination_hub_id
service_type_id
payment_method_id
weight
freight_charge
cod_amount
order_date
pickup_date
promised_delivery_date
actual_delivery_date
current_status
created_at
updated_at
```

### `shipment_tracking_events`

```text
event_id
shipment_id
event_time
event_type
hub_id
courier_id
vehicle_id
status_reason_id
created_at
updated_at
```

---

## 5.4. Dữ liệu bẩn cố ý cài cắm

Mục đích: demo Data Quality Gate và ETL xử lý ngoại lệ.

| Loại lỗi                         |   Tỷ lệ gợi ý | Cách xử lý mong muốn     |
| ---------------------------------- | ----------------: | ---------------------------- |
| `weight <= 0`                    |                1% | Quarantine                   |
| `delivery_date < order_date`     |                1% | Quarantine                   |
| Thiếu `courier_id`              |                2% | Map Unknown hoặc warning    |
| Thiếu `hub_id` ở một event    |                1% | Map Unknown                  |
| Số điện thoại sai định dạng |                2% | Chuẩn hóa hoặc cảnh báo |
| Khách hàng đổi hạng           | một số bản ghi | Test SCD2                    |
| Shipper chuyển hub                | một số bản ghi | Test SCD2                    |

---

## 6. Thiết kế Data Warehouse

## 6.1. Schema khuyến nghị

```text
stg     -- staging / bronze
silver  -- cleansed / conformed
gold    -- dimensional model
meta    -- audit / control / quarantine
```

---

## 6.2. Sơ đồ Galaxy Schema tổng thể

```mermaid
flowchart LR
    FS[Fact_Shipment_Lifecycle] --> DD[Dim_Date]
    FS --> DC[Dim_Customer]
    FS --> DSR[Dim_Service_Type]
    FS --> DH[Dim_Hub]
    FS --> DPM[Dim_Payment_Method]
    FS --> DST[Dim_Status_Reason]

    FT[Fact_Tracking_Events] --> DD
    FT --> DH
    FT --> DCO[Dim_Courier]
    FT --> DV[Dim_Vehicle]
    FT --> DST

    FHS[Fact_Daily_Hub_Snapshot] --> DD
    FHS --> DH
```

---

# 7. Thiết kế chi tiết các Dimensions

## 7.1. Quy ước chung cho tất cả Dimensions

Mọi dimension nên có:

- `Surrogate_Key` viết tắt `_SK`
- `Natural_Key` viết tắt `_NK`
- `Is_Current`
- `Valid_From`
- `Valid_To` nếu là SCD Type 2
- dòng Unknown với `SK = -1`

### Dòng Unknown chuẩn

```text
SK = -1
NK = 'UNKNOWN'
Description = 'Unknown'
Is_Current = 1
```

---

## 7.2. Danh sách 8 Dimensions

| Dimension              | Loại SCD                         | Ghi chú                             |
| ---------------------- | --------------------------------- | ------------------------------------ |
| `Dim_Date`           | Static                            | Date key dạng `YYYYMMDD`          |
| `Dim_Customer`       | SCD Type 2                        | Theo dõi hạng khách, loại khách |
| `Dim_Courier`        | SCD Type 2                        | Theo dõi hub, contract type         |
| `Dim_Hub`            | SCD Type 1 hoặc SCD2 đơn giản | Ưu tiên đơn giản                |
| `Dim_Service_Type`   | SCD Type 1                        | Kèm SLA chuẩn                      |
| `Dim_Vehicle`        | SCD Type 1                        | Loại phương tiện                 |
| `Dim_Status_Reason`  | SCD Type 1                        | Trạng thái / lý do                |
| `Dim_Payment_Method` | SCD Type 1                        | Payment / COD flag                   |

---

## 7.3. Thiết kế gợi ý từng Dimension

### `Dim_Date`

```text
date_key            -- YYYYMMDD
full_date
year
quarter
month
month_name
week
day_of_week
day_of_month
is_weekend
is_peak_season
```

### `Dim_Customer`

```text
customer_sk
customer_nk
customer_name
customer_type
membership_tier
city
district
valid_from
valid_to
is_current
row_hash
```

### `Dim_Courier`

```text
courier_sk
courier_nk
courier_name
hub_sk
contract_type
vehicle_sk
valid_from
valid_to
is_current
row_hash
```

### `Dim_Hub`

```text
hub_sk
hub_nk
hub_name
hub_level
city
district
capacity
is_current
```

### `Dim_Service_Type`

```text
service_type_sk
service_type_nk
service_name
sla_hours
is_express
```

### `Dim_Vehicle`

```text
vehicle_sk
vehicle_nk
vehicle_type
capacity_kg
```

### `Dim_Status_Reason`

```text
status_reason_sk
status_code
status_name
reason_code
reason_name
event_category
```

### `Dim_Payment_Method`

```text
payment_method_sk
payment_method_nk
payment_method_name
payer_type
has_cod
```

---

# 8. Thiết kế chi tiết các Fact Tables

## 8.1. Fact số 1: `Fact_Shipment_Lifecycle`

### Loại Fact

**Accumulating Snapshot Fact**

### Grain

Mỗi dòng là **một vận đơn**.

### Mục đích

Theo dõi toàn bộ vòng đời của shipment:

- đặt hàng
- lấy hàng
- phân loại
- giao hàng
- hoàn hàng
- SLA

### Khóa và cột gợi ý

```text
shipment_sk
shipment_number
sender_customer_sk
receiver_customer_sk
origin_hub_sk
destination_hub_sk
service_type_sk
payment_method_sk
current_status_sk

order_date_key
pickup_date_key
first_sorting_date_key
last_sorting_date_key
first_delivery_attempt_date_key
final_delivery_date_key
return_date_key
promised_delivery_date_key

weight
freight_charge
cod_amount
distance_km

pickup_to_sorting_hours
sorting_to_delivery_hours
total_lead_time_hours

is_delivered
is_returned
is_cancelled
is_on_time
is_sla_breached

created_at
updated_at
batch_id
```

### Logic nạp

- Dùng **MERGE / UPSERT** theo `shipment_number`.
- Khi shipment có mốc mới thì update fact.
- Tính lại:
  - `total_lead_time_hours`
  - `is_on_time`
  - `is_sla_breached`

---

## 8.2. Fact số 2: `Fact_Tracking_Events`

### Loại Fact

**Transaction Fact**

### Grain

Mỗi dòng là **một sự kiện scan / tracking event**.

### Mục đích

Phân tích:

- lưu lượng hàng qua hub
- thời gian ngâm hàng
- năng suất shipper
- điểm nghẽn mạng lưới

### Khóa và cột gợi ý

```text
tracking_event_sk
event_id_nk
shipment_sk
event_date_key
event_timestamp
event_type_sk
hub_sk
courier_sk
vehicle_sk
status_reason_sk

event_sequence
scan_source
batch_id
loaded_at
```

### Logic nạp

- INSERT là chính.
- Nếu có chỉnh sửa event từ nguồn, cần UPSERT theo `event_id_nk`.
- Không được nạp trùng event khi chạy lại pipeline.

---

## 8.3. Fact số 3: `Fact_Daily_Hub_Snapshot`

### Loại Fact

**Periodic Snapshot Fact**

### Grain

Mỗi dòng là **một hub trong một ngày**.

### Mục đích

Theo dõi tồn đọng và năng lực vận hành của hub theo ngày.

### Cột gợi ý

```text
snapshot_date_key
hub_sk

inbound_count
outbound_count
backlog_count
pending_delivery_count
return_count
cod_held_amount

batch_id
snapshot_generated_at
```

### Định nghĩa nghiệp vụ bắt buộc

#### `backlog_count`

Số đơn **đang nằm tại hub** và **chưa hoàn tất giao** tại thời điểm cuối ngày.

#### `inbound_count`

Số tracking event loại **inbound** phát sinh trong ngày tại hub.

#### `outbound_count`

Số tracking event loại **outbound** phát sinh trong ngày tại hub.

#### `cod_held_amount`

Tổng tiền COD của các đơn đang nằm tại hub và chưa được đối soát.

### Logic nạp

- Có thể tính lại theo ngày.
- Phải idempotent.
- Nếu chạy lại cho cùng `snapshot_date_key`, cần overwrite hoặc delete-then-insert theo ngày/hub.

---

# 9. Thiết kế tầng Meta / Audit

## 9.1. Các bảng meta bắt buộc

### `meta.etl_watermark`

```text
table_name
last_extracted_timestamp
last_batch_id
updated_at
```

### `meta.etl_batch_log`

```text
batch_id
table_name
run_start
run_end
rows_extracted
rows_loaded
rows_rejected
status
error_message
```

### `meta.data_quarantine`

```text
quarantine_id
batch_id
source_table
record_id
rule_name
severity
error_message
payload
created_at
is_resolved
```

---

## 9.2. Nguyên tắc audit

Mỗi lần chạy pipeline phải có:

- `batch_id`
- số dòng extract
- số dòng load
- số dòng lỗi
- trạng thái thành công / thất bại

Nếu không có audit, pipeline sẽ khó được xem là enterprise-grade.

---

# 10. Chiến lược ETL chi tiết

## 10.1. Sơ đồ ETL tổng thể

```mermaid
flowchart LR
    W[Read Watermark] --> EX[Extract Incremental]
    EX --> STG[Load Staging]
    STG --> DQ[Data Quality Checks]
    DQ --> SIL[Load Silver]
    DQ --> QUAR[Quarantine]
    SIL --> DIM[Load Dimensions]
    SIL --> FACT[Load Facts]
    DIM --> GOLD[Gold]
    FACT --> GOLD
    GOLD --> AUD[Update Batch Log]
```

---

## 10.2. Nguyên tắc Extract

### Incremental Extract

Chỉ extract dữ liệu:

```sql
WHERE updated_at > :last_extracted_timestamp
```

hoặc:

```sql
WHERE updated_at >= :last_extracted_timestamp
```

tùy chiến lược chống sót dữ liệu.

### Yêu cầu

- Phải đọc watermark từ `meta.etl_watermark`.
- Sau khi chạy xong thành công mới được update watermark.
- Extract phải ghi log vào `meta.etl_batch_log`.

---

## 10.3. Nguyên tắc Staging

### Có hai cách

#### Cách 1: đơn giản cho đồ án

- Truncate staging table theo batch hiện tại.
- Insert dữ liệu mới extract.

Ưu điểm:

- dễ làm
- dễ debug

Nhược điểm:

- ít tính audit hơn.

#### Cách 2: chuẩn enterprise hơn

- Append vào staging với `batch_id`.
- Giữ lại lịch sử raw.
- Có thể truy vết.

Khuyến nghị:

- nếu đủ thời gian, nên làm cách 2.

---

## 10.4. Nguyên tắc Silver / Cleansed

Silver là nơi:

- trim chuỗi
- chuẩn hóa số điện thoại
- chuẩn hóa mã tỉnh
- xử lý NULL
- chuẩn hóa trạng thái
- tạo `row_hash` cho SCD2

---

## 10.5. Data Quality Gate

### Phân loại mức độ lỗi

| Severity | Hành vi                                     |
| -------- | -------------------------------------------- |
| Blocker  | Không load Gold, chuyển vào quarantine    |
| Warning  | Vẫn load, nhưng gắn cờ hoặc map Unknown |

### Danh sách rule gợi ý

| Rule                              | Bảng           | Severity |
| --------------------------------- | --------------- | -------- |
| `weight <= 0`                   | shipments       | Blocker  |
| `delivery_date < order_date`    | shipments       | Blocker  |
| thiếu `sender_customer_id`     | shipments       | Blocker  |
| thiếu `courier_id` trong event | tracking_events | Warning  |
| thiếu `hub_id` trong event     | tracking_events | Warning  |
| event_time nhỏ hơn order_time   | tracking_events | Blocker  |
| trạng thái không hợp lệ      | shipments       | Blocker  |

### Bảng quarantine cần lưu

- record gốc
- tên rule
- mức độ lỗi
- thông báo lỗi
- batch_id

---

## 10.6. Load Dimensions

## Nguyên tắc chung

Thứ tự load dimensions nên là:

1. `Dim_Date`
2. `Dim_Hub`
3. `Dim_Vehicle`
4. `Dim_Service_Type`
5. `Dim_Status_Reason`
6. `Dim_Payment_Method`
7. `Dim_Customer`
8. `Dim_Courier`

Vì `Dim_Courier` có thể phụ thuộc vào `Dim_Hub` và `Dim_Vehicle`.

---

## SCD Type 2 cho Customer và Courier

### Quy trình chuẩn

1. Lấy bản ghi hiện hành trong dimension:
   - `is_current = 1`
2. So sánh thuộc tính nghiệp vụ bằng `row_hash`.
3. Nếu hash giống:
   - không làm gì.
4. Nếu hash khác:
   - update bản hiện hành:
     - `is_current = 0`
     - `valid_to = current_timestamp`
   - insert bản mới:
     - `is_current = 1`
     - `valid_from = current_timestamp`
     - `valid_to = '9999-12-31'`

### Lưu ý quan trọng

`row_hash` phải được tính từ các cột nghiệp vụ đã chuẩn hóa.

Ví dụ:

```text
MD5(
  COALESCE(customer_name, '') || '|' ||
  COALESCE(customer_type, '') || '|' ||
  COALESCE(membership_tier, '') || '|' ||
  COALESCE(city, '')
)
```

Không nên đưa `updated_at` vào hash nếu không cần thiết.

---

## SCD Type 2 phải được dùng đúng cách khi join vào Fact

### Sai

Chỉ join theo natural key rồi lấy bản hiện hành.

### Đúng

Join theo thời điểm nghiệp vụ:

```sql
FROM fact f
JOIN dim_customer c
  ON f.sender_customer_nk = c.customer_nk
 AND f.order_date >= c.valid_from
 AND f.order_date < c.valid_to
```

Hoặc với event:

```sql
FROM fact_tracking_events f
JOIN dim_courier c
  ON f.courier_nk = c.courier_nk
 AND f.event_timestamp >= c.valid_from
 AND f.event_timestamp < c.valid_to
```

Đây là điểm rất quan trọng để chứng minh SCD2 có giá trị thật.

---

## 10.7. Load Facts

### Fact_Tracking_Events

- Load theo event.
- Có unique constraint trên `event_id_nk` để tránh trùng.
- Nên insert hoặc upsert.

### Fact_Shipment_Lifecycle

- Load bằng MERGE / UPSERT.
- Natural key là `shipment_number`.
- Khi có milestone mới thì cập nhật.
- Tính lại các chỉ số SLA.

### Fact_Daily_Hub_Snapshot

- Tính theo ngày.
- Có thể delete theo `snapshot_date_key` rồi insert lại.
- Hoặc dùng UPSERT theo khóa `(snapshot_date_key, hub_sk)`.

---

# 11. Thiết kế Index và tối ưu hiệu năng

## 11.1. Index khuyến nghị

### Dimensions

Index trên:

- natural key
- `is_current`
- `valid_from`, `valid_to` với SCD2

### Facts

#### `Fact_Tracking_Events`

```text
(event_date_key)
(hub_sk, event_date_key)
(courier_sk, event_date_key)
(shipment_sk)
```

#### `Fact_Shipment_Lifecycle`

```text
(order_date_key)
(final_delivery_date_key)
(origin_hub_sk, destination_hub_sk)
(service_type_sk)
(current_status_sk)
```

#### `Fact_Daily_Hub_Snapshot`

```text
(snapshot_date_key)
(hub_sk, snapshot_date_key)
```

---

## 11.2. Partitioning

Chỉ nên làm ở mức vừa phải.

### Ứng viên tốt nhất

- `Fact_Tracking_Events`

### Chiến lược

Partition theo tháng dựa trên `event_date_key` hoặc `event_timestamp`.

### Mục tiêu khi bảo vệ

Không cần chứng minh tốc độ tăng quá nhiều, mà cần show:

- partition pruning
- query plan gọn hơn
- giảm scan không cần thiết

---

## 11.3. Pre-aggregation / Views

Nên tạo các views hoặc materialized views cho:

- OTD theo ngày / hub / service type
- Dwell time theo hub
- Backlog theo ngày
- Return rate theo courier
- Volume theo khung giờ

Ví dụ:

```text
gold.mv_daily_hub_performance
gold.mv_courier_performance
gold.v_slaship_summary
```

---

## 11.4. Benchmark nên làm thế nào?

So sánh 3 kịch bản:

1. Query trực tiếp trên OLTP với nhiều join.
2. Query trên Gold chưa index.
3. Query trên Gold đã index / materialized view.

Chỉ số nên chụp lại:

- `EXPLAIN ANALYZE`
- thời gian chạy trung bình sau vài lần
- số dòng được scan
- index used / seq scan

---

# 12. Thiết kế Dashboard / BI Layer

## 12.1. Ba trang dashboard chính

### Trang 1: Executive & SLA Overview

Các chỉ số:

- tổng số đơn
- tổng doanh thu cước
- tổng COD
- OTD %
- SLA breach %
- số đơn hoàn
- xu hướng theo ngày

### Trang 2: Hub & Network Bottleneck

Các chỉ số:

- volume theo hub
- inbound/outbound theo ngày
- backlog cuối ngày
- dwell time trung bình
- top hub nghẽn

### Trang 3: Courier Performance & Return Analysis

Các chỉ số:

- số đơn giao theo courier
- tỷ lệ giao đúng hạn
- tỷ lệ hoàn
- lý do hoàn
- năng suất theo thời gian
- lịch sử courier chuyển hub nếu có SCD2

---

## 12.2. Định nghĩa KPI chuẩn

### On-Time Delivery Rate

```text
OTD = số đơn giao đúng hạn / tổng số đơn đã giao
```

### SLA Breached

```text
actual_delivery_timestamp > promised_delivery_timestamp
```

### Return Rate

```text
số đơn hoàn / tổng số đơn đã vào hệ thống
```

### Dwell Time

```text
thời gian giữa inbound scan và outbound scan tại một hub
```

### Backlog

```text
số đơn chưa giao xong và đang nằm tại hub tại cuối ngày
```

---

# 13. Thiết kế Airflow DAG

## 13.1. Sơ đồ DAG tổng thể

```mermaid
flowchart TD
    A[preflight_checks] --> B[extract_incremental]
    B --> C[data_quality_gate]
    C --> D[load_dimensions]
    D --> E[load_facts]
    E --> F[refresh_views]
    F --> G[update_watermark]
```

---

## 13.2. Chi tiết tasks

### Nhóm 1: Preflight

- kiểm tra kết nối OLTP
- kiểm tra kết nối DWH
- kiểm tra bảng meta tồn tại

### Nhóm 2: Extract

- extract `customers`
- extract `hubs`
- extract `couriers`
- extract `shipments`
- extract `shipment_tracking_events`

### Nhóm 3: Data Quality

- chạy blocker rules
- chạy warning rules
- insert lỗi vào quarantine

### Nhóm 4: Load Dimensions

- load `Dim_Date`
- load `Dim_Hub`
- load `Dim_Service_Type`
- load `Dim_Vehicle`
- load `Dim_Status_Reason`
- load `Dim_Payment_Method`
- load `Dim_Customer`
- load `Dim_Courier`

### Nhóm 5: Load Facts

- load `Fact_Tracking_Events`
- load `Fact_Shipment_Lifecycle`
- load `Fact_Daily_Hub_Snapshot`

### Nhóm 6: Post Load

- refresh views / materialized views
- update watermark
- audit row count

---

## 13.3. Nguyên tắc bắt buộc với Airflow

DAG phải:

- idempotent
- chạy lại được
- không tạo dữ liệu trùng
- có retries
- có logging
- có failure alert hoặc log rõ lỗi
- dùng connections thay vì hardcode credentials

---

# 14. Cấu trúc thư mục dự án khuyến nghị

```text
express-delivery-dwh/
│
├── config/
│   ├── db_config.py
│   └── settings.py
│
├── sql/
│   ├── oltp/
│   │   ├── 01_create_oltp_tables.sql
│   │   └── 02_seed_constraints.sql
│   │
│   ├── dwh/
│   │   ├── 01_create_schemas.sql
│   │   ├── 02_create_meta_tables.sql
│   │   ├── 03_create_staging_tables.sql
│   │   ├── 04_create_silver_tables.sql
│   │   ├── 05_create_dimensions.sql
│   │   ├── 06_create_facts.sql
│   │   └── 07_create_indexes.sql
│   │
│   └── analytics/
│       ├── 01_create_views.sql
│       └── 02_create_materialized_views.sql
│
├── scripts/
│   ├── generate_oltp_data.py
│   ├── extract_incremental.py
│   ├── data_quality_checks.py
│   ├── load_dimensions.py
│   ├── load_facts.py
│   ├── refresh_views.py
│   └── run_local_pipeline.py
│
├── dags/
│   └── express_delivery_dwh_dag.py
│
├── docker/
│   ├── docker-compose.yml
│   └── requirements.txt
│
└── docs/
    ├── EXPRESS_DELIVERY_DWH_MASTER_PLAN.md
    ├── data_dictionary.md
    └── kpi_definitions.md
```

---

# 15. Thứ tự triển khai thực tế

## Milestone 1: Chạy được local pipeline

Mục tiêu:

- tạo OLTP
- sinh dữ liệu
- tạo schemas DWH
- chạy ETL từ source đến Gold

Đầu ra:

- dữ liệu có trong 3 facts
- dimensions có dữ liệu
- query thử ra số đúng

---

## Milestone 2: ETL chuẩn enterprise

Mục tiêu:

- incremental load hoạt động
- data quality gate hoạt động
- quarantine có dữ liệu
- SCD2 hoạt động
- merge fact hoạt động

Đầu ra:

- chạy lần 2 không nạp trùng
- có batch log
- có watermark

---

## Milestone 3: Dashboard

Mục tiêu:

- tạo semantic views
- kết nối Power BI / Superset
- dựng 3 trang báo cáo

Đầu ra:

- dashboard SLA
- dashboard Hub bottleneck
- dashboard Courier performance

---

## Milestone 4: Airflow Docker

Mục tiêu:

- đóng gói pipeline
- chạy DAG end-to-end
- chứng minh idempotency

Đầu ra:

- DAG chạy thành công
- log rõ ràng
- rerun không lỗi

---

# 16. Definition of Done

Một phần được xem là xong khi:

## OLTP

- Có đủ bảng nguồn.
- Có dữ liệu hợp lệ và dữ liệu bẩn có kiểm soát.
- Có `created_at`, `updated_at`.

## DWH

- Có 3 tầng rõ ràng.
- Có dimensions và facts đúng grain.
- Có dòng Unknown trong dimensions.
- Có indexes cơ bản.

## ETL

- Extract incremental được.
- Có batch log.
- Có quarantine.
- Có SCD2.
- Có merge/upsert facts.
- Rerun không trùng dữ liệu.

## BI

- Dashboard hiển thị được các KPI chính.
- KPI có định nghĩa rõ ràng.

## Airflow

- DAG chạy được end-to-end.
- Có task failures handling.
- Không hardcode thông tin kết nối.

---

# 17. Checklist chống bị hỏi xoáy khi bảo vệ

Bạn cần trả lời rõ các câu sau:

### Về mô hình

- Grain của từng fact là gì?
- Vì sao chọn Accumulating Snapshot cho shipment?
- Vì sao chọn Transaction Fact cho tracking events?
- Vì sao cần Periodic Snapshot cho hub?

### Về ETL

- Incremental dựa vào cột nào?
- Làm sao biết lần trước extract đến đâu?
- Rerun pipeline có trùng dữ liệu không?
- Dữ liệu lỗi bị xử lý thế nào?

### Về SCD2

- Vì sao phải có Valid_From / Valid_To?
- Khi join vào fact, join theo bản ghi hiện hành hay bản ghi lịch sử?
- Nếu shipper chuyển hub thì báo cáo cũ có bị sai không?

### Về hiệu năng

- Vì sao star schema phù hợp hơn truy vấn OLTP trực tiếp?
- Index nào đã tạo và vì sao?
- Có bằng chứng execution plan không?

### Về nghiệp vụ

- OTD tính thế nào?
- Backlog tính thế nào?
- Dwell time tính thế nào?
- Return rate tính thế nào?

---

# 18. Rủi ro thường gặp và cách tránh

| Rủi ro                           | Hậu quả                                 | Cách tránh                            |
| --------------------------------- | ----------------------------------------- | --------------------------------------- |
| Không có `updated_at`         | Không incremental được                | Bắt buộc có trong OLTP               |
| SCD2 join sai                     | Báo cáo lịch sử bị sai               | Join theo thời điểm nghiệp vụ      |
| Snapshot không định nghĩa rõ | Số backlog thiếu tin cậy               | Định nghĩa backlog ngay từ đầu    |
| Pipeline không idempotent        | Chạy lại bị trùng dữ liệu           | Dùng MERGE/UPSERT, delete theo ngày   |
| Quá tập trung vào partition    | Không kịp phần core                    | Chỉ làm sau khi pipeline chạy đúng |
| Thiếu audit                      | Khó debug, khó bảo vệ                 | Có batch log ngay từ đầu            |
| Dữ liệu bẩn không kiểm soát | Không biết lỗi do pipeline hay do data | Sinh data bẩn có chủ đích          |

---

# 19. Bản rút gọn để triển khai ngay

Nếu bạn cần một phiên bản “làm ngay”, hãy bám theo thứ tự sau:

## Bước 1

Tạo OLTP với:

- customers
- hubs
- couriers
- service_types
- vehicles
- shipments
- shipment_tracking_events

## Bước 2

Tạo schemas:

- stg
- silver
- gold
- meta

## Bước 3

Tạo 8 dimensions trong Gold.

## Bước 4

Tạo 3 facts trong Gold.

## Bước 5

Viết script:

- generate data
- extract incremental
- data quality
- load dims
- load facts
- run pipeline

## Bước 6

Tạo views cho BI.

## Bước 7

Dựng dashboard.

## Bước 8

Đóng gói Airflow.

---

# 20. Kết luận triển khai

Tài liệu này là bản thiết kế tổng thể cuối cùng để bạn bám sát khi làm đồ án.

Nguyên tắc cốt lõi cần nhớ:

1. **Đúng grain trước, tối ưu sau.**
2. **Pipeline phải idempotent.**
3. **SCD2 phải phục vụ phân tích lịch sử, không phải chỉ để đẹp.**
4. **Data quality phải có rule và quarantine.**
5. **Mọi thay đổi phải audit được.**
6. **Dashboard phải dựa trên KPI có định nghĩa rõ ràng.**

Nếu bạn triển khai đúng theo tài liệu này, đồ án sẽ có hình dáng của một hệ thống Data Warehouse enterprise thực thụ, không chỉ là bài tập mô phỏng đơn thuần.

---

Nếu bạn muốn, bước tiếp theo mình có thể viết tiếp cho bạn một trong các file sau:

1. **`01_create_oltp_tables.sql`** – script tạo toàn bộ OLTP
2. **`02_create_dwh_schemas.sql`** – script tạo schemas, meta, staging
3. **`03_create_dimensions.sql`** – script tạo 8 Dimensions
4. **`04_create_facts.sql`** – script tạo 3 Facts
5. **`generate_oltp_data.py`** – script sinh dữ liệu mẫu
6. **`EXPRESS_DELIVERY_DWH_PRESENTATION.md`** – outline slide bảo vệ

Bạn chỉ cần nói: **“Viết tiếp phần 1”** hoặc **“Viết tiếp OLTP SQL”** là mình làm tiế

Bản thiết kế và bộ mã nguồn đầy đủ dưới đây bao phủ toàn bộ quy trình xây dựng hệ thống **Data Warehouse cho Công ty Chuyển phát nhanh (Express Delivery)** theo chuẩn kiến trúc doanh nghiệp 3-Tier kết hợp Medallion, từ khâu mô hình hóa nghiệp vụ đến lập trình ETL tự động và tối ưu hiệu năng.

---

## PHẦN 1: CẤU TRÚC THƯ MỤC HOÀN CHỈNH & LỘ TRÌNH 6 PHASE DOANH NGHIỆP

### 1.1. Hoàn thiện cấu trúc nhánh `bi_analytics/`

Bổ sung các tệp phục vụ Semantic Layer (Data Mart Views), DAX Measures và kịch bản kết nối Power BI:

```text
express-delivery-dwh/
├── docker-compose.yml                  # Dựng Airflow + SQL Server (OLTP & DWH) + Postgres
├── requirements.txt                    # Thư viện Python (pyodbc, faker, apache-airflow...)
├── docs/
│   ├── 01_bus_matrix.xlsx              # Ma trận Bus & Định nghĩa KPI
│   ├── 02_architecture_diagram.png     # Sơ đồ kiến trúc 3-Tier / Medallion
│   └── 03_star_schema_erd.png          # Sơ đồ 3 Fact + 8 Dim
├── oltp_source/
│   ├── ddl_create_oltp.sql             # Khởi tạo 12 bảng OLTP chuẩn 3NF
│   └── generate_fake_data.py           # Sinh dữ liệu vận đơn + log quét mã vạch (CDC/Timestamp)
├── dwh_scripts/
│   ├── 01_staging_bronze/
│   │   └── ddl_staging_and_audit.sql   # DDL bảng Staging, Watermark & Audit Log
│   ├── 02_core_silver_gold/
│   │   └── ddl_star_schema.sql         # DDL 8 Dim + 3 Fact (Transaction, Accumulating, Periodic)
│   ├── 03_stored_procedures_etl/
│   │   └── sp_etl_master.sql           # Logic SCD Type 2, Upsert Accumulating Fact, Snapshot
│   └── 04_performance_tuning/
│       └── indexing_and_partition.sql  # Partitioning, Clustered Columnstore Index (CCI), Benchmark
├── airflow/
│   └── dags/
│       └── express_dwh_etl_dag.py      # DAG điều phối ETL, kiểm tra Data Quality & ghi Audit Log
└── bi_analytics/
    ├── 01_semantic_views_gold.sql      # Views chuẩn hóa tên cột thân thiện cho Power BI Import/DirectQuery
    ├── 02_powerbi_dax_measures.dax     # Công thức DAX tính SLA, On-Time Delivery, Thời gian trung chuyển
    └── 03_dashboard_wireframe.md       # Đặc tả 3 trang Dashboard: Executive, Operations, Hub Performance
```

### 1.2. Lộ trình 6 Phase Thiết kế Kho dữ liệu Doanh nghiệp (Kimball Lifecycle)

| Phase             | Tên Giai đoạn                                       | Trọng tâm kỹ thuật & Sản phẩm bàn giao (Deliverables)                                                                                                                                                                                                                                                   |
| ----------------- | ------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Phase 1** | **Business Requirements & Dimensional Modeling** | Xác định bài toán cốt lõi ngành Logistics (Tối ưu thời gian toàn trình - Lead time, Tỷ lệ giao đúng hạn - SLA, Tắc nghẽn tại Hub). Thiết lập **Enterprise Bus Matrix**, xác định **Grain** (độ mịn) cho 3 bảng Fact và thiết kế **8 Conformed Dimensions**. |
| **Phase 2** | **3-Tier / Medallion Architecture Design**       | Thiết kế luồng dữ liệu 3 tầng vật lý:                                                                                                                                                                                                                                                                  |

• **Tier 1 (Staging / Bronze):** Hứng dữ liệu thô nguyên bản từ OLTP qua cơ chế Incremental Load (High-Watermark), truncate trước mỗi lần chạy.

• **Tier 2 (Core DWH / Silver):** Làm sạch, chuẩn hóa mã lỗi, xử lý **SCD Type 2** (theo dõi lịch sử đổi cấp bậc khách hàng, đổi quản lý/loại Bưu cục) và cập nhật các bảng Fact.

• **Tier 3 (Semantic Layer / Gold):** Các Views tổng hợp, tính sẵn cờ SLA phục vụ trực tiếp cho Power BI. |
| **Phase 3** | **OLTP Simulation & High-Volume Data Generation** | Xây dựng hệ thống DB nguồn chuẩn 3NF (12 bảng) mô phỏng thực tế hoạt động tạo vận đơn (Waybill), phân tuyến, quét mã vạch (Barcode Scan) tại các Bưu cục/Hub và đối soát COD. Viết script Python sinh dữ liệu có tính logic thời gian (`Created -> Picked -> Hub In -> Hub Out -> Delivered`). |
| **Phase 4** | **ETL Pipeline & Orchestration (Airflow)** | Xây dựng cơ chế **Metadata-Driven ETL**: Bảng `etl_watermark` lưu mốc thời gian trích xuất cuối cùng, bảng `etl_audit_log` lưu số dòng Insert/Update và trạng thái lỗi. Lập trình **Apache Airflow DAG** điều phối: `Extract OLTP -> Load Staging -> Transform Dims -> Transform Facts -> Data Quality Check`. |
| **Phase 5** | **Performance Tuning & Storage Optimization** | Áp dụng kỹ thuật tối ưu cho bảng Fact lớn:

• **Clustered Columnstore Index (CCI):** Nén dữ liệu dạng cột giúp giảm 70-80% dung lượng và tăng tốc truy vấn Aggregation gấp 10 lần.

• **Table Partitioning:** Phân vùng bảng Fact theo tháng (`date_key`) để tối ưu hóa cơ chế Partition Elimination. |
| **Phase 6** | **BI Semantic Layer & Analytics (Power BI)** | Xây dựng lớp Views Gold, kết nối vào Power BI xây dựng mô hình Galaxy Schema (Fact Constellation). Viết bộ đo lường DAX nâng cao phân tích hiệu suất Shipper, Bottleneck tại Mega Hub và doanh thu COD. |

---

## PHẦN 2: THIẾT KẾ MÔ HÌNH ĐA CHIỀU (BUS MATRIX, 3 FACTS & 8 DIMS)

### 2.1. Enterprise Bus Matrix

| Quy trình Nghiệp vụ (Bảng Fact) | Loại Fact (Kimball)                 | Độ mịn (Grain)                                                          | `dim_date`    | `dim_time` | `dim_customer` | `dim_hub`                 | `dim_courier` | `dim_service` | `dim_vehicle` | `dim_status` |
| ----------------------------------- | ------------------------------------ | -------------------------------------------------------------------------- | --------------- | ------------ | ---------------- | --------------------------- | --------------- | --------------- | --------------- | -------------- |
| **1. `fact_shipment_lifecycle**`  | **Accumulating Snapshot Fact** | **1 dòng / 1 Vận đơn (Waybill)** trong suốt vòng đời         | X (Nhiều mốc) |              | X (Gửi & Nhận) | X (Gốc & Đích)           | X (Lấy & Giao) | X               |                 | X (Hiện tại) |
| **2. `fact_tracking_events**`     | **Transaction Fact**           | **1 dòng / 1 lần quét mã vạch (Scan Event)** trên hành trình | X               | X            |                  | X (Hiện tại & Tiếp theo) | X               |                 | X               | X              |
| **3. `fact_daily_hub_snapshot**`  | **Periodic Snapshot Fact**     | **1 dòng / 1 Bưu cục / 1 Ngày** chốt tồn cuối ngày           | X               |              |                  | X                           |                 | X               |                 |                |

*Lưu ý kỹ thuật:* Bảng `fact_shipment_lifecycle` sử dụng kỹ thuật **Role-Playing Dimension** (1 bảng Dim đóng nhiều vai trò bằng cách nối nhiều khóa ngoại, ví dụ: `origin_hub_sk` và `dest_hub_sk` cùng trỏ về `dim_hub`; `order_date_key`, `pickup_date_key`, `delivered_date_key` cùng trỏ về `dim_date`).

---

## PHẦN 3: MÃ NGUỒN CHI TIẾT TOÀN DỰ ÁN

### 3.1. Hạ tầng Docker (`docker-compose.yml` & `requirements.txt`)

**File: `requirements.txt**`

```text
apache-airflow==2.9.1
apache-airflow-providers-microsoft-mssql==3.7.0
pyodbc==5.1.0
Faker==25.2.0
pandas==2.2.2

```

**File: `docker-compose.yml**`

```yaml
version: '3.8'

services:
  mssql-server:
    image: mcr.microsoft.com/mssql/server:2022-latest
    container_name: express_mssql
    environment:
      - ACCEPT_EULA=Y
      - MSSQL_SA_PASSWORD=ExpressDWH@2026Strong!
      - MSSQL_PID=Developer
    ports:
      - "1433:1433"
    volumes:
      - mssql_data:/var/opt/mssql
      - ./oltp_source:/scripts/oltp_source
      - ./dwh_scripts:/scripts/dwh_scripts
    restart: always

  postgres-airflow:
    image: postgres:15
    container_name: airflow_postgres
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - pg_data:/var/lib/postgresql/data

  airflow-webserver:
    image: apache/airflow:2.9.1-python3.11
    container_name: express_airflow
    depends_on:
      - postgres-airflow
      - mssql-server
    environment:
      - AIRFLOW__CORE__EXECUTOR=LocalExecutor
      - AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
      - AIRFLOW__CORE__LOAD_EXAMPLES=False
      - _PIP_ADDITIONAL_REQUIREMENTS=pyodbc apache-airflow-providers-microsoft-mssql Faker pandas
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./oltp_source:/opt/airflow/oltp_source
      - ./dwh_scripts:/opt/airflow/dwh_scripts
    ports:
      - "8080:8080"
    command: bash -c "airflow db migrate && airflow users create --username admin --password admin --firstname Phong --lastname Nguyen --role Admin --email admin@express.vn && (airflow webserver & airflow scheduler)"

volumes:
  mssql_data:
  pg_data:

```

---

### 3.2. Nguồn OLTP Chuẩn 3NF & Sinh dữ liệu (`oltp_source/`)

**File: `oltp_source/ddl_create_oltp.sql**`

```sql
USE master;
GO
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ExpressOLTP')
    CREATE DATABASE ExpressOLTP;
GO
USE ExpressOLTP;
GO

-- 1. Bảng Tỉnh/Thành phố
CREATE TABLE Provinces (
    province_id INT PRIMARY KEY IDENTITY(1,1),
    province_code VARCHAR(10) NOT NULL UNIQUE,
    province_name NVARCHAR(100) NOT NULL,
    region NVARCHAR(50) NOT NULL -- Miền Bắc, Miền Trung, Miền Nam
);

-- 2. Bảng Quận/Huyện
CREATE TABLE Districts (
    district_id INT PRIMARY KEY IDENTITY(1,1),
    province_id INT NOT NULL FOREIGN KEY REFERENCES Provinces(province_id),
    district_name NVARCHAR(100) NOT NULL
);

-- 3. Bảng Bưu cục / Kho trung chuyển (Hubs)
CREATE TABLE Hubs (
    hub_id INT PRIMARY KEY IDENTITY(1,1),
    hub_code VARCHAR(20) NOT NULL UNIQUE,
    hub_name NVARCHAR(150) NOT NULL,
    hub_type VARCHAR(30) NOT NULL, -- 'MEGA_HUB', 'SORTING_CENTER', 'LOCAL_POST'
    district_id INT NOT NULL FOREIGN KEY REFERENCES Districts(district_id),
    capacity_packages INT NOT NULL,
    manager_name NVARCHAR(100),
    updated_at DATETIME2 DEFAULT SYSDATETIME()
);

-- 4. Bảng Khách hàng (Shop B2B & Khách lẻ B2C)
CREATE TABLE Customers (
    customer_id INT PRIMARY KEY IDENTITY(1,1),
    customer_code VARCHAR(30) NOT NULL UNIQUE,
    full_name NVARCHAR(150) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    customer_type VARCHAR(20) NOT NULL, -- 'B2B_SHOP', 'B2C_RETAIL'
    tier VARCHAR(20) NOT NULL,          -- 'BRONZE', 'SILVER', 'GOLD', 'VIP'
    district_id INT NOT NULL FOREIGN KEY REFERENCES Districts(district_id),
    created_at DATETIME2 DEFAULT SYSDATETIME(),
    updated_at DATETIME2 DEFAULT SYSDATETIME()
);

-- 5. Bảng Tài xế / Bưu tá (Couriers)
CREATE TABLE Couriers (
    courier_id INT PRIMARY KEY IDENTITY(1,1),
    courier_code VARCHAR(20) NOT NULL UNIQUE,
    full_name NVARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    courier_type VARCHAR(30) NOT NULL, -- 'LAST_MILE_BIKE', 'LINEHAUL_TRUCK'
    base_hub_id INT NOT NULL FOREIGN KEY REFERENCES Hubs(hub_id),
    status VARCHAR(20) DEFAULT 'ACTIVE',
    updated_at DATETIME2 DEFAULT SYSDATETIME()
);

-- 6. Bảng Phương tiện vận tải (Vehicles)
CREATE TABLE Vehicles (
    vehicle_id INT PRIMARY KEY IDENTITY(1,1),
    license_plate VARCHAR(20) NOT NULL UNIQUE,
    vehicle_type VARCHAR(30) NOT NULL, -- 'MOTORBIKE', 'TRUCK_2T', 'TRUCK_8T', 'CONTAINER'
    capacity_kg DECIMAL(10,2) NOT NULL,
    updated_at DATETIME2 DEFAULT SYSDATETIME()
);

-- 7. Bảng Loại dịch vụ (Service Types)
CREATE TABLE ServiceTypes (
    service_id INT PRIMARY KEY IDENTITY(1,1),
    service_code VARCHAR(20) NOT NULL UNIQUE,
    service_name NVARCHAR(100) NOT NULL, -- 'Hỏa tốc', 'Nhanh', 'Tiết kiệm'
    transport_mode VARCHAR(20) NOT NULL, -- 'AIR', 'ROAD'
    sla_hours INT NOT NULL,              -- Thời gian cam kết toàn trình (giờ)
    base_price DECIMAL(12,2) NOT NULL,
    updated_at DATETIME2 DEFAULT SYSDATETIME()
);

-- 8. Bảng Danh mục Mã trạng thái (Status Catalog)
CREATE TABLE StatusCatalog (
    status_id INT PRIMARY KEY,
    status_code VARCHAR(30) NOT NULL UNIQUE,
    status_name NVARCHAR(100) NOT NULL,
    status_category VARCHAR(30) NOT NULL -- 'IN_PROGRESS', 'COMPLETED', 'EXCEPTION'
);

-- 9. Bảng Vận đơn gốc (Waybills / Shipments)
CREATE TABLE Shipments (
    shipment_id BIGINT PRIMARY KEY IDENTITY(1,1),
    waybill_number VARCHAR(30) NOT NULL UNIQUE,
    sender_id INT NOT NULL FOREIGN KEY REFERENCES Customers(customer_id),
    receiver_id INT NOT NULL FOREIGN KEY REFERENCES Customers(customer_id),
    origin_hub_id INT NOT NULL FOREIGN KEY REFERENCES Hubs(hub_id),
    dest_hub_id INT NOT NULL FOREIGN KEY REFERENCES Hubs(hub_id),
    service_id INT NOT NULL FOREIGN KEY REFERENCES ServiceTypes(service_id),
    pickup_courier_id INT NULL FOREIGN KEY REFERENCES Couriers(courier_id),
    delivery_courier_id INT NULL FOREIGN KEY REFERENCES Couriers(courier_id),
    current_status_id INT NOT NULL FOREIGN KEY REFERENCES StatusCatalog(status_id),
    weight_gram INT NOT NULL,
    shipping_fee DECIMAL(12,2) NOT NULL,
    cod_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
    created_at DATETIME2 NOT NULL,
    picked_up_at DATETIME2 NULL,
    origin_hub_in_at DATETIME2 NULL,
    dest_hub_in_at DATETIME2 NULL,
    delivered_at DATETIME2 NULL,
    updated_at DATETIME2 NOT NULL
);

-- 10. Bảng Kiện hàng chi tiết (Shipment Packages)
CREATE TABLE ShipmentPackages (
    package_id BIGINT PRIMARY KEY IDENTITY(1,1),
    shipment_id BIGINT NOT NULL FOREIGN KEY REFERENCES Shipments(shipment_id),
    length_cm INT NOT NULL,
    width_cm INT NOT NULL,
    height_cm INT NOT NULL,
    volumetric_weight_gram AS ((length_cm * width_cm * height_cm) / 6) PERSISTED
);

-- 11. Bảng Lịch sử quét mã vạch (Tracking Events)
CREATE TABLE ShipmentTrackingEvents (
    event_id BIGINT PRIMARY KEY IDENTITY(1,1),
    shipment_id BIGINT NOT NULL FOREIGN KEY REFERENCES Shipments(shipment_id),
    status_id INT NOT NULL FOREIGN KEY REFERENCES StatusCatalog(status_id),
    current_hub_id INT NOT NULL FOREIGN KEY REFERENCES Hubs(hub_id),
    next_hub_id INT NULL FOREIGN KEY REFERENCES Hubs(hub_id),
    courier_id INT NULL FOREIGN KEY REFERENCES Couriers(courier_id),
    vehicle_id INT NULL FOREIGN KEY REFERENCES Vehicles(vehicle_id),
    processing_seconds INT NOT NULL DEFAULT 0, -- Thời gian xử lý tại khâu này
    event_timestamp DATETIME2 NOT NULL
);

-- 12. Bảng Đối soát COD (COD Transactions)
CREATE TABLE CODTransactions (
    cod_tx_id BIGINT PRIMARY KEY IDENTITY(1,1),
    shipment_id BIGINT NOT NULL FOREIGN KEY REFERENCES Shipments(shipment_id),
    collected_amount DECIMAL(12,2) NOT NULL,
    collected_at DATETIME2 NOT NULL,
    settled_to_sender_at DATETIME2 NULL
);
GO

```

**File: `oltp_source/generate_fake_data.py**`

```python
import random
from datetime import datetime, timedelta
import pyodbc
from faker import Faker

fake = Faker('vi_VN')

CONN_STR = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=ExpressOLTP;"
    "UID=sa;"
    "PWD=ExpressDWH@2026Strong!;"
    "TrustServerCertificate=yes;"
)

def seed_oltp_data(num_shipments=500):
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    # 1. Seed Master Data nếu chưa có
    cursor.execute("SELECT COUNT(*) FROM StatusCatalog")
    if cursor.fetchone()[0] == 0:
        statuses = [
            (100, 'CREATED', N'Mới tạo đơn', 'IN_PROGRESS'),
            (200, 'PICKED_UP', N'Lấy hàng thành công', 'IN_PROGRESS'),
            (300, 'ORIGIN_HUB_IN', N'Nhập kho bưu cục gửi', 'IN_PROGRESS'),
            (400, 'IN_TRANSIT', N'Đang trung chuyển Đường trục', 'IN_PROGRESS'),
            (500, 'DEST_HUB_IN', N'Nhập kho bưu cục phát', 'IN_PROGRESS'),
            (600, 'OUT_FOR_DELIVERY', N'Đang giao hàng', 'IN_PROGRESS'),
            (700, 'DELIVERED', N'Giao hàng thành công', 'COMPLETED'),
            (800, 'RETURNED', N'Hoàn hàng', 'EXCEPTION')
        ]
        cursor.executemany("INSERT INTO StatusCatalog VALUES (?, ?, ?, ?)", statuses)

        provinces = [
            ('HN', N'Hà Nội', N'Miền Bắc'),
            ('DN', N'Đà Nẵng', N'Miền Trung'),
            ('SG', N'TP. Hồ Chí Minh', N'Miền Nam')
        ]
        cursor.executemany("INSERT INTO Provinces (province_code, province_name, region) VALUES (?, ?, ?)", provinces)
        conn.commit()

        cursor.execute("SELECT province_id FROM Provinces")
        prov_ids = [r[0] for r in cursor.fetchall()]
        for pid in prov_ids:
            for i in range(1, 4):
                cursor.execute("INSERT INTO Districts (province_id, district_name) VALUES (?, ?)", (pid, f"Quận {i}"))
        conn.commit()

        cursor.execute("SELECT district_id FROM Districts")
        dist_ids = [r[0] for r in cursor.fetchall()]

        # Seed Hubs
        hub_types = ['MEGA_HUB', 'SORTING_CENTER', 'LOCAL_POST']
        for i in range(1, 10):
            cursor.execute("""
                INSERT INTO Hubs (hub_code, hub_name, hub_type, district_id, capacity_packages, manager_name)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (f"HUB{i:03d}", f"Bưu cục Trung tâm {i}", random.choice(hub_types), random.choice(dist_ids), random.randint(5000, 20000), fake.name()))

        # Seed ServiceTypes
        services = [
            ('EXP_AIR', N'Chuyển phát Hỏa tốc Hàng không', 'AIR', 24, 65000),
            ('FAST_RD', N'Chuyển phát Nhanh Liên tỉnh', 'ROAD', 48, 35000),
            ('ECO_RD', N'Chuyển phát Tiết kiệm', 'ROAD', 96, 20000)
        ]
        cursor.executemany("""
            INSERT INTO ServiceTypes (service_code, service_name, transport_mode, sla_hours, base_price)
            VALUES (?, ?, ?, ?, ?)
        """, services)

        # Seed Vehicles
        v_types = [('MOTORBIKE', 50), ('TRUCK_2T', 2000), ('TRUCK_8T', 8000)]
        for i in range(1, 21):
            vt, cap = random.choice(v_types)
            cursor.execute("INSERT INTO Vehicles (license_plate, vehicle_type, capacity_kg) VALUES (?, ?, ?)",
                           (f"29C-{random.randint(10000, 99999)}", vt, cap))

        # Seed Customers
        for i in range(1, 101):
            cursor.execute("""
                INSERT INTO Customers (customer_code, full_name, phone, customer_type, tier, district_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (f"CUST{i:04d}", fake.name(), fake.phone_number()[:15],
                  random.choice(['B2B_SHOP', 'B2C_RETAIL']),
                  random.choice(['BRONZE', 'SILVER', 'GOLD', 'VIP']),
                  random.choice(dist_ids)))

        conn.commit()

        # Seed Couriers
        cursor.execute("SELECT hub_id FROM Hubs")
        hub_ids = [r[0] for r in cursor.fetchall()]
        for i in range(1, 31):
            cursor.execute("""
                INSERT INTO Couriers (courier_code, full_name, phone, courier_type, base_hub_id)
                VALUES (?, ?, ?, ?, ?)
            """, (f"EMP{i:04d}", fake.name(), fake.phone_number()[:15],
                  random.choice(['LAST_MILE_BIKE', 'LINEHAUL_TRUCK']), random.choice(hub_ids)))
        conn.commit()

    # Lấy ID danh mục để sinh Shipments
    cursor.execute("SELECT customer_id FROM Customers")
    cust_ids = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT hub_id FROM Hubs")
    hub_ids = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT service_id, base_price FROM ServiceTypes")
    services = cursor.fetchall()
    cursor.execute("SELECT courier_id FROM Couriers")
    courier_ids = [r[0] for r in cursor.fetchall()]
    cursor.execute("SELECT vehicle_id FROM Vehicles")
    vehicle_ids = [r[0] for r in cursor.fetchall()]

    base_time = datetime.now() - timedelta(days=15)

    for _ in range(num_shipments):
        sender, receiver = random.sample(cust_ids, 2)
        orig_hub, dest_hub = random.sample(hub_ids, 2)
        srv_id, base_price = random.choice(services)
        p_courier, d_courier = random.sample(courier_ids, 2)
        weight = random.randint(200, 15000)
        fee = float(base_price) + (weight / 1000.0) * 5000
        cod = random.choice([0, 0, random.randint(100, 2000) * 1000])

        t_created = base_time + timedelta(hours=random.randint(1, 240), minutes=random.randint(0, 59))
        t_pickup = t_created + timedelta(hours=random.randint(1, 6))
        t_orig_in = t_pickup + timedelta(hours=random.randint(1, 4))
        t_dest_in = t_orig_in + timedelta(hours=random.randint(8, 36))

        # Mô phỏng 80% đơn đã giao xong, 15% đang đi trên đường, 5% hoàn
        outcome = random.choices(['DELIVERED', 'IN_TRANSIT', 'RETURNED'], weights=[80, 15, 5])[0]
        if outcome == 'DELIVERED':
            t_deliv = t_dest_in + timedelta(hours=random.randint(2, 18))
            status_id = 700
            t_updated = t_deliv
        elif outcome == 'RETURNED':
            t_deliv = t_dest_in + timedelta(hours=random.randint(12, 48))
            status_id = 800
            t_updated = t_deliv
        else:
            t_deliv = None
            status_id = 500
            t_updated = t_dest_in

        wb_num = f"VN{random.randint(100000000, 999999999)}EX"

        cursor.execute("""
            INSERT INTO Shipments (
                waybill_number, sender_id, receiver_id, origin_hub_id, dest_hub_id,
                service_id, pickup_courier_id, delivery_courier_id, current_status_id,
                weight_gram, shipping_fee, cod_amount,
                created_at, picked_up_at, origin_hub_in_at, dest_hub_in_at, delivered_at, updated_at
            ) OUTPUT INSERTED.shipment_id
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (wb_num, sender, receiver, orig_hub, dest_hub, srv_id, p_courier, d_courier,
              status_id, weight, fee, cod, t_created, t_pickup, t_orig_in, t_dest_in, t_deliv, t_updated))

        ship_id = cursor.fetchone()[0]

        # Sinh chuỗi Tracking Events tương ứng
        events = [
            (ship_id, 100, orig_hub, orig_hub, None, None, 60, t_created),
            (ship_id, 200, orig_hub, orig_hub, p_courier, random.choice(vehicle_ids), 300, t_pickup),
            (ship_id, 300, orig_hub, dest_hub, p_courier, random.choice(vehicle_ids), 900, t_orig_in),
            (ship_id, 500, dest_hub, dest_hub, d_courier, random.choice(vehicle_ids), 1200, t_dest_in),
        ]
        if t_deliv:
            events.append((ship_id, status_id, dest_hub, None, d_courier, random.choice(vehicle_ids), 450, t_deliv))

        cursor.executemany("""
            INSERT INTO ShipmentTrackingEvents (
                shipment_id, status_id, current_hub_id, next_hub_id,
                courier_id, vehicle_id, processing_seconds, event_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, events)

    conn.commit()
    conn.close()
    print(f"Đã sinh thành công {num_shipments} vận đơn và chuỗi sự kiện quét mã vạch!")

if __name__ == "__main__":
    seed_oltp_data(500)

```

---

### 3.3. Tầng Staging / Bronze & Metadata Control (`dwh_scripts/01_staging_bronze/`)

**File: `dwh_scripts/01_staging_bronze/ddl_staging_and_audit.sql**`

```sql
USE master;
GO
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'ExpressDWH')
    CREATE DATABASE ExpressDWH;
GO
USE ExpressDWH;
GO

-- Tạo 3 Schemas tương ứng 3-Tier / Medallion Architecture
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg') EXEC('CREATE SCHEMA stg');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dwh') EXEC('CREATE SCHEMA dwh');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold') EXEC('CREATE SCHEMA gold');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'meta') EXEC('CREATE SCHEMA meta');
GO

-- 1. Bảng quản lý High-Watermark cho Incremental Load
CREATE TABLE meta.etl_watermark (
    table_name VARCHAR(100) PRIMARY KEY,
    last_extracted_at DATETIME2 NOT NULL
);

INSERT INTO meta.etl_watermark VALUES
('Customers', '2000-01-01 00:00:00'),
('Hubs', '2000-01-01 00:00:00'),
('Couriers', '2000-01-01 00:00:00'),
('Vehicles', '2000-01-01 00:00:00'),
('ServiceTypes', '2000-01-01 00:00:00'),
('Shipments', '2000-01-01 00:00:00'),
('ShipmentTrackingEvents', '2000-01-01 00:00:00');

-- 2. Bảng ghi nhận lịch sử chạy ETL (Audit Log)
CREATE TABLE meta.etl_audit_log (
    audit_id INT PRIMARY KEY IDENTITY(1,1),
    dag_run_id VARCHAR(100),
    step_name VARCHAR(100) NOT NULL,
    rows_processed INT DEFAULT 0,
    status VARCHAR(20) NOT NULL, -- 'SUCCESS', 'FAILED'
    start_time DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    end_time DATETIME2 NULL,
    error_message NVARCHAR(MAX) NULL
);

-- 3. Các bảng Staging (Bronze Layer)
CREATE TABLE stg.Customers (
    customer_id INT,
    customer_code VARCHAR(30),
    full_name NVARCHAR(150),
    phone VARCHAR(20),
    customer_type VARCHAR(20),
    tier VARCHAR(20),
    district_name NVARCHAR(100),
    province_name NVARCHAR(100),
    region NVARCHAR(50),
    updated_at DATETIME2
);

CREATE TABLE stg.Hubs (
    hub_id INT,
    hub_code VARCHAR(20),
    hub_name NVARCHAR(150),
    hub_type VARCHAR(30),
    district_name NVARCHAR(100),
    province_name NVARCHAR(100),
    region NVARCHAR(50),
    capacity_packages INT,
    manager_name NVARCHAR(100),
    updated_at DATETIME2
);

CREATE TABLE stg.Couriers (
    courier_id INT,
    courier_code VARCHAR(20),
    full_name NVARCHAR(100),
    phone VARCHAR(20),
    courier_type VARCHAR(30),
    base_hub_id INT,
    status VARCHAR(20),
    updated_at DATETIME2
);

CREATE TABLE stg.Vehicles (
    vehicle_id INT,
    license_plate VARCHAR(20),
    vehicle_type VARCHAR(30),
    capacity_kg DECIMAL(10,2),
    updated_at DATETIME2
);

CREATE TABLE stg.ServiceTypes (
    service_id INT,
    service_code VARCHAR(20),
    service_name NVARCHAR(100),
    transport_mode VARCHAR(20),
    sla_hours INT,
    base_price DECIMAL(12,2),
    updated_at DATETIME2
);

CREATE TABLE stg.Shipments (
    shipment_id BIGINT,
    waybill_number VARCHAR(30),
    sender_id INT,
    receiver_id INT,
    origin_hub_id INT,
    dest_hub_id INT,
    service_id INT,
    pickup_courier_id INT,
    delivery_courier_id INT,
    current_status_id INT,
    weight_gram INT,
    shipping_fee DECIMAL(12,2),
    cod_amount DECIMAL(12,2),
    created_at DATETIME2,
    picked_up_at DATETIME2,
    origin_hub_in_at DATETIME2,
    dest_hub_in_at DATETIME2,
    delivered_at DATETIME2,
    updated_at DATETIME2
);

CREATE TABLE stg.ShipmentTrackingEvents (
    event_id BIGINT,
    shipment_id BIGINT,
    status_id INT,
    current_hub_id INT,
    next_hub_id INT,
    courier_id INT,
    vehicle_id INT,
    processing_seconds INT,
    event_timestamp DATETIME2
);
GO

```

---

### 3.4. Tầng Core DWH / Silver - 8 Dimensions & 3 Facts (`dwh_scripts/02_core_silver_gold/`)

**File: `dwh_scripts/02_core_silver_gold/ddl_star_schema.sql**`

```sql
USE ExpressDWH;
GO

-- ============================================================================
-- PHẦN A: KHỞI TẠO 8 BẢNG DIMENSIONS (CONFORMED DIMENSIONS)
-- ============================================================================

-- 1. DIM_DATE
CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD (VD: 20261103)
    full_date DATE NOT NULL,
    day_of_week TINYINT NOT NULL,
    day_name NVARCHAR(20) NOT NULL,
    day_of_month TINYINT NOT NULL,
    month_num TINYINT NOT NULL,
    month_name NVARCHAR(20) NOT NULL,
    quarter_num TINYINT NOT NULL,
    year_num SMALLINT NOT NULL,
    is_weekend BIT NOT NULL
);

-- 2. DIM_TIME (Độ mịn theo Phút trong ngày)
CREATE TABLE dwh.dim_time (
    time_key INT PRIMARY KEY, -- HHMM (VD: 0830, 1445)
    hour_24 TINYINT NOT NULL,
    minute_num TINYINT NOT NULL,
    shift_name NVARCHAR(30) NOT NULL -- 'Ca Sáng (06-14h)', 'Ca Chiều (14-22h)', 'Ca Đêm (22-06h)'
);

-- 3. DIM_CUSTOMER (SCD Type 2 - Theo dõi lịch sử nâng/hạ hạng Tier của Shop)
CREATE TABLE dwh.dim_customer (
    customer_sk INT PRIMARY KEY IDENTITY(1,1),
    customer_nk INT NOT NULL, -- customer_id từ OLTP
    customer_code VARCHAR(30) NOT NULL,
    full_name NVARCHAR(150) NOT NULL,
    phone VARCHAR(20),
    customer_type VARCHAR(20),
    tier VARCHAR(20),
    district_name NVARCHAR(100),
    province_name NVARCHAR(100),
    region NVARCHAR(50),
    effective_from DATETIME2 NOT NULL,
    effective_to DATETIME2 NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BIT NOT NULL DEFAULT 1
);

-- 4. DIM_HUB (SCD Type 2 - Theo dõi thay đổi công suất / loại Bưu cục / Quản lý)
CREATE TABLE dwh.dim_hub (
    hub_sk INT PRIMARY KEY IDENTITY(1,1),
    hub_nk INT NOT NULL, -- hub_id từ OLTP
    hub_code VARCHAR(20) NOT NULL,
    hub_name NVARCHAR(150) NOT NULL,
    hub_type VARCHAR(30) NOT NULL,
    district_name NVARCHAR(100),
    province_name NVARCHAR(100),
    region NVARCHAR(50),
    capacity_packages INT,
    manager_name NVARCHAR(100),
    effective_from DATETIME2 NOT NULL,
    effective_to DATETIME2 NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BIT NOT NULL DEFAULT 1
);

-- 5. DIM_COURIER (SCD Type 2 - Theo dõi lịch sử điều chuyển Bưu cục của Shipper)
CREATE TABLE dwh.dim_courier (
    courier_sk INT PRIMARY KEY IDENTITY(1,1),
    courier_nk INT NOT NULL,
    courier_code VARCHAR(20) NOT NULL,
    full_name NVARCHAR(100) NOT NULL,
    courier_type VARCHAR(30) NOT NULL,
    base_hub_nk INT NOT NULL,
    status VARCHAR(20),
    effective_from DATETIME2 NOT NULL,
    effective_to DATETIME2 NOT NULL DEFAULT '9999-12-31 23:59:59',
    is_current BIT NOT NULL DEFAULT 1
);

-- 6. DIM_SERVICE_TYPE (SCD Type 1)
CREATE TABLE dwh.dim_service_type (
    service_sk INT PRIMARY KEY IDENTITY(1,1),
    service_nk INT NOT NULL UNIQUE,
    service_code VARCHAR(20) NOT NULL,
    service_name NVARCHAR(100) NOT NULL,
    transport_mode VARCHAR(20) NOT NULL,
    sla_hours INT NOT NULL,
    base_price DECIMAL(12,2) NOT NULL
);

-- 7. DIM_VEHICLE (SCD Type 1)
CREATE TABLE dwh.dim_vehicle (
    vehicle_sk INT PRIMARY KEY IDENTITY(1,1),
    vehicle_nk INT NOT NULL UNIQUE,
    license_plate VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(30) NOT NULL,
    capacity_kg DECIMAL(10,2) NOT NULL
);

-- 8. DIM_STATUS (Static / SCD Type 1)
CREATE TABLE dwh.dim_status (
    status_sk INT PRIMARY KEY IDENTITY(1,1),
    status_nk INT NOT NULL UNIQUE,
    status_code VARCHAR(30) NOT NULL,
    status_name NVARCHAR(100) NOT NULL,
    status_category VARCHAR(30) NOT NULL
);
GO

-- ============================================================================
-- PHẦN B: KHỞI TẠO 3 BẢNG FACTS (GALAXY SCHEMA / FACT CONSTELLATION)
-- ============================================================================

-- FACT 1: ACCUMULATING SNAPSHOT FACT (Vòng đời Vận đơn & Đo lường SLA)
CREATE TABLE dwh.fact_shipment_lifecycle (
    shipment_sk BIGINT PRIMARY KEY IDENTITY(1,1),
    shipment_nk BIGINT NOT NULL UNIQUE,
    waybill_number VARCHAR(30) NOT NULL, -- Degenerate Dimension
    -- Role-playing Date Keys
    created_date_key INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_date(date_key),
    picked_up_date_key INT NULL FOREIGN KEY REFERENCES dwh.dim_date(date_key),
    origin_hub_in_date_key INT NULL FOREIGN KEY REFERENCES dwh.dim_date(date_key),
    dest_hub_in_date_key INT NULL FOREIGN KEY REFERENCES dwh.dim_date(date_key),
    delivered_date_key INT NULL FOREIGN KEY REFERENCES dwh.dim_date(date_key),
    -- Role-playing Customer & Hub & Courier Surrogate Keys
    sender_customer_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_customer(customer_sk),
    receiver_customer_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_customer(customer_sk),
    origin_hub_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_hub(hub_sk),
    dest_hub_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_hub(hub_sk),
    pickup_courier_sk INT NULL FOREIGN KEY REFERENCES dwh.dim_courier(courier_sk),
    delivery_courier_sk INT NULL FOREIGN KEY REFERENCES dwh.dim_courier(courier_sk),
    service_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_service_type(service_sk),
    current_status_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_status(status_sk),
    -- Measures (Độ đo tài chính & khối lượng)
    weight_gram INT NOT NULL,
    shipping_fee DECIMAL(12,2) NOT NULL,
    cod_amount DECIMAL(12,2) NOT NULL,
    -- Lag Measures (Độ trễ giữa các chặng tính bằng Giờ)
    pickup_lead_time_hrs DECIMAL(8,2) NULL,
    transit_lead_time_hrs DECIMAL(8,2) NULL,
    last_mile_lead_time_hrs DECIMAL(8,2) NULL,
    total_lead_time_hrs DECIMAL(8,2) NULL,
    -- Cờ SLA
    is_sla_breached BIT NULL,
    updated_at DATETIME2 NOT NULL
);

-- FACT 2: TRANSACTION FACT (Chi tiết từng lần quét mã vạch trên hành trình)
CREATE TABLE dwh.fact_tracking_events (
    event_sk BIGINT IDENTITY(1,1),
    event_nk BIGINT NOT NULL,
    shipment_nk BIGINT NOT NULL,
    event_date_key INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_date(date_key),
    event_time_key INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_time(time_key),
    current_hub_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_hub(hub_sk),
    next_hub_sk INT NULL FOREIGN KEY REFERENCES dwh.dim_hub(hub_sk),
    courier_sk INT NULL FOREIGN KEY REFERENCES dwh.dim_courier(courier_sk),
    vehicle_sk INT NULL FOREIGN KEY REFERENCES dwh.dim_vehicle(vehicle_sk),
    status_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_status(status_sk),
    processing_seconds INT NOT NULL,
    event_count INT NOT NULL DEFAULT 1,
    CONSTRAINT PK_fact_tracking_events PRIMARY KEY NONCLUSTERED (event_sk, event_date_key)
);

-- FACT 3: PERIODIC SNAPSHOT FACT (Chốt sức tải & tồn kho Bưu cục mỗi ngày)
CREATE TABLE dwh.fact_daily_hub_snapshot (
    snapshot_date_key INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_date(date_key),
    hub_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_hub(hub_sk),
    service_sk INT NOT NULL FOREIGN KEY REFERENCES dwh.dim_service_type(service_sk),
    inbound_packages_count INT NOT NULL DEFAULT 0,
    outbound_packages_count INT NOT NULL DEFAULT 0,
    delivered_packages_count INT NOT NULL DEFAULT 0,
    backlog_packages_count INT NOT NULL DEFAULT 0, -- Hàng tồn đọng chưa xuất
    avg_processing_seconds DECIMAL(10,2) NOT NULL DEFAULT 0,
    CONSTRAINT PK_fact_daily_hub_snapshot PRIMARY KEY (snapshot_date_key, hub_sk, service_sk)
);
GO

```

---

### 3.5. Stored Procedures ETL: SCD Type 2 & Upsert Accumulating Fact (`dwh_scripts/03_stored_procedures_etl/`)

**File: `dwh_scripts/03_stored_procedures_etl/sp_etl_master.sql**`

```sql
USE ExpressDWH;
GO

-- 1. Thủ tục sinh tự động Dim_Date (2024 - 2028) và Dim_Time + Unknown Member (-1)
CREATE OR ALTER PROCEDURE dwh.sp_populate_static_dims
AS
BEGIN
    SET NOCOUNT ON;

    -- Chèn dòng Unknown Member (-1) cho các Dim để tránh lỗi NULL Foreign Key
    IF NOT EXISTS (SELECT 1 FROM dwh.dim_date WHERE date_key = -1)
        INSERT INTO dwh.dim_date VALUES (-1, '1900-01-01', 1, 'Unknown', 1, 1, 'Unknown', 1, 1900, 0);

    IF NOT EXISTS (SELECT 1 FROM dwh.dim_customer WHERE customer_sk = -1)
    BEGIN
        SET IDENTITY_INSERT dwh.dim_customer ON;
        INSERT INTO dwh.dim_customer (customer_sk, customer_nk, customer_code, full_name, customer_type, tier, effective_from)
        VALUES (-1, -1, 'UNKNOWN', N'Khách vãng lai / Chưa xác định', 'UNKNOWN', 'NONE', '1900-01-01');
        SET IDENTITY_INSERT dwh.dim_customer OFF;
    END

    IF NOT EXISTS (SELECT 1 FROM dwh.dim_hub WHERE hub_sk = -1)
    BEGIN
        SET IDENTITY_INSERT dwh.dim_hub ON;
        INSERT INTO dwh.dim_hub (hub_sk, hub_nk, hub_code, hub_name, hub_type, effective_from)
        VALUES (-1, -1, 'UNK', N'Chưa xác định', 'UNKNOWN', '1900-01-01');
        SET IDENTITY_INSERT dwh.dim_hub OFF;
    END

    IF NOT EXISTS (SELECT 1 FROM dwh.dim_courier WHERE courier_sk = -1)
    BEGIN
        SET IDENTITY_INSERT dwh.dim_courier ON;
        INSERT INTO dwh.dim_courier (courier_sk, courier_nk, courier_code, full_name, courier_type, base_hub_nk, effective_from)
        VALUES (-1, -1, 'UNK', N'Chưa phân công', 'UNKNOWN', -1, '1900-01-01');
        SET IDENTITY_INSERT dwh.dim_courier OFF;
    END

    IF NOT EXISTS (SELECT 1 FROM dwh.dim_vehicle WHERE vehicle_sk = -1)
    BEGIN
        SET IDENTITY_INSERT dwh.dim_vehicle ON;
        INSERT INTO dwh.dim_vehicle (vehicle_sk, vehicle_nk, license_plate, vehicle_type, capacity_kg)
        VALUES (-1, -1, 'UNKNOWN', 'UNKNOWN', 0);
        SET IDENTITY_INSERT dwh.dim_vehicle OFF;
    END

    -- Sinh Dim_Date nếu chưa có dữ liệu
    IF (SELECT COUNT(*) FROM dwh.dim_date) <= 1
    BEGIN
        DECLARE @d DATE = '2024-01-01';
        WHILE @d <= '2028-12-31'
        BEGIN
            INSERT INTO dwh.dim_date VALUES (
                YEAR(@d)*10000 + MONTH(@d)*100 + DAY(@d),
                @d,
                DATEPART(WEEKDAY, @d),
                DATENAME(WEEKDAY, @d),
                DAY(@d),
                MONTH(@d),
                DATENAME(MONTH, @d),
                DATEPART(QUARTER, @d),
                YEAR(@d),
                CASE WHEN DATEPART(WEEKDAY, @d) IN (1, 7) THEN 1 ELSE 0 END
            );
            SET @d = DATEADD(DAY, 1, @d);
        END
    END

    -- Sinh Dim_Time (1440 phút trong ngày)
    IF NOT EXISTS (SELECT 1 FROM dwh.dim_time)
    BEGIN
        DECLARE @h INT = 0, @m INT = 0;
        WHILE @h < 24
        BEGIN
            SET @m = 0;
            WHILE @m < 60
            BEGIN
                INSERT INTO dwh.dim_time VALUES (
                    @h * 100 + @m,
                    @h,
                    @m,
                    CASE 
                        WHEN @h >= 6 AND @h < 14 THEN N'Ca Sáng (06-14h)'
                        WHEN @h >= 14 AND @h < 22 THEN N'Ca Chiều (14-22h)'
                        ELSE N'Ca Đêm (22-06h)'
                    END
                );
                SET @m = @m + 1;
            END
            SET @h = @h + 1;
        END
    END
END;
GO

-- 2. Thủ tục ETL SCD Type 2 cho Dim_Customer, Dim_Hub, Dim_Courier và SCD1
CREATE OR ALTER PROCEDURE dwh.sp_etl_dimensions
AS
BEGIN
    SET NOCOUNT ON;

    -- 2.1. SCD Type 2: DIM_CUSTOMER (Theo dõi thay đổi Tier hoặc Khu vực)
    -- Đóng bản ghi cũ nếu có thay đổi
    UPDATE d
    SET d.effective_to = s.updated_at,
        d.is_current = 0
    FROM dwh.dim_customer d
    INNER JOIN stg.Customers s ON d.customer_nk = s.customer_id
    WHERE d.is_current = 1
      AND (d.tier <> s.tier OR d.customer_type <> s.customer_type OR ISNULL(d.district_name,'') <> ISNULL(s.district_name,''));

    -- Chèn bản ghi mới (Khách hàng mới hoàn toàn HOẶC khách hàng vừa bị đóng bản ghi cũ)
    INSERT INTO dwh.dim_customer (
        customer_nk, customer_code, full_name, phone, customer_type,
        tier, district_name, province_name, region, effective_from, effective_to, is_current
    )
    SELECT 
        s.customer_id, s.customer_code, s.full_name, s.phone, s.customer_type,
        s.tier, s.district_name, s.province_name, s.region,
        s.updated_at, '9999-12-31 23:59:59', 1
    FROM stg.Customers s
    LEFT JOIN dwh.dim_customer d ON s.customer_id = d.customer_nk AND d.is_current = 1
    WHERE d.customer_sk IS NULL;

    -- 2.2. SCD Type 2: DIM_HUB (Theo dõi thay đổi Hub_Type, Capacity, Manager)
    UPDATE d
    SET d.effective_to = s.updated_at,
        d.is_current = 0
    FROM dwh.dim_hub d
    INNER JOIN stg.Hubs s ON d.hub_nk = s.hub_id
    WHERE d.is_current = 1
      AND (d.hub_type <> s.hub_type OR d.capacity_packages <> s.capacity_packages OR ISNULL(d.manager_name,'') <> ISNULL(s.manager_name,''));

    INSERT INTO dwh.dim_hub (
        hub_nk, hub_code, hub_name, hub_type, district_name, province_name,
        region, capacity_packages, manager_name, effective_from, effective_to, is_current
    )
    SELECT 
        s.hub_id, s.hub_code, s.hub_name, s.hub_type, s.district_name, s.province_name,
        s.region, s.capacity_packages, s.manager_name, s.updated_at, '9999-12-31 23:59:59', 1
    FROM stg.Hubs s
    LEFT JOIN dwh.dim_hub d ON s.hub_id = d.hub_nk AND d.is_current = 1
    WHERE d.hub_sk IS NULL;

    -- 2.3. SCD Type 2: DIM_COURIER
    UPDATE d
    SET d.effective_to = s.updated_at,
        d.is_current = 0
    FROM dwh.dim_courier d
    INNER JOIN stg.Couriers s ON d.courier_nk = s.courier_id
    WHERE d.is_current = 1
      AND (d.base_hub_nk <> s.base_hub_id OR d.courier_type <> s.courier_type OR d.status <> s.status);

    INSERT INTO dwh.dim_courier (
        courier_nk, courier_code, full_name, courier_type, base_hub_nk, status, effective_from, effective_to, is_current
    )
    SELECT 
        s.courier_id, s.courier_code, s.full_name, s.courier_type, s.base_hub_id, s.status,
        s.updated_at, '9999-12-31 23:59:59', 1
    FROM stg.Couriers s
    LEFT JOIN dwh.dim_courier d ON s.courier_id = d.courier_nk AND d.is_current = 1
    WHERE d.courier_sk IS NULL;

    -- 2.4. SCD Type 1: DIM_SERVICE_TYPE & DIM_VEHICLE & DIM_STATUS
    MERGE dwh.dim_service_type AS target
    USING stg.ServiceTypes AS source ON target.service_nk = source.service_id
    WHEN MATCHED THEN
        UPDATE SET service_name = source.service_name, sla_hours = source.sla_hours, base_price = source.base_price
    WHEN NOT MATCHED THEN
        INSERT (service_nk, service_code, service_name, transport_mode, sla_hours, base_price)
        VALUES (source.service_id, source.service_code, source.service_name, source.transport_mode, source.sla_hours, source.base_price);

    MERGE dwh.dim_vehicle AS target
    USING stg.Vehicles AS source ON target.vehicle_nk = source.vehicle_id
    WHEN MATCHED THEN
        UPDATE SET vehicle_type = source.vehicle_type, capacity_kg = source.capacity_kg
    WHEN NOT MATCHED THEN
        INSERT (vehicle_nk, license_plate, vehicle_type, capacity_kg)
        VALUES (source.vehicle_id, source.license_plate, source.vehicle_type, source.capacity_kg);

    MERGE dwh.dim_status AS target
    USING ExpressOLTP.dbo.StatusCatalog AS source ON target.status_nk = source.status_id
    WHEN MATCHED THEN
        UPDATE SET status_name = source.status_name, status_category = source.status_category
    WHEN NOT MATCHED THEN
        INSERT (status_nk, status_code, status_name, status_category)
        VALUES (source.status_id, source.status_code, source.status_name, source.status_category);
END;
GO

-- 3. Thủ tục ETL nạp 3 bảng Fact (Accumulating Upsert, Transaction Insert, Periodic Snapshot)
CREATE OR ALTER PROCEDURE dwh.sp_etl_facts
AS
BEGIN
    SET NOCOUNT ON;

    -- 3.1. Nạp/Cập nhật FACT_SHIPMENT_LIFECYCLE (Accumulating Snapshot)
    MERGE dwh.fact_shipment_lifecycle AS target
    USING (
        SELECT 
            s.shipment_id,
            s.waybill_number,
            ISNULL(YEAR(s.created_at)*10000 + MONTH(s.created_at)*100 + DAY(s.created_at), -1) AS created_date_key,
            ISNULL(YEAR(s.picked_up_at)*10000 + MONTH(s.picked_up_at)*100 + DAY(s.picked_up_at), -1) AS picked_up_date_key,
            ISNULL(YEAR(s.origin_hub_in_at)*10000 + MONTH(s.origin_hub_in_at)*100 + DAY(s.origin_hub_in_at), -1) AS origin_hub_in_date_key,
            ISNULL(YEAR(s.dest_hub_in_at)*10000 + MONTH(s.dest_hub_in_at)*100 + DAY(s.dest_hub_in_at), -1) AS dest_hub_in_date_key,
            ISNULL(YEAR(s.delivered_at)*10000 + MONTH(s.delivered_at)*100 + DAY(s.delivered_at), -1) AS delivered_date_key,
            ISNULL(cs.customer_sk, -1) AS sender_customer_sk,
            ISNULL(cr.customer_sk, -1) AS receiver_customer_sk,
            ISNULL(ho.hub_sk, -1) AS origin_hub_sk,
            ISNULL(hd.hub_sk, -1) AS dest_hub_sk,
            ISNULL(cp.courier_sk, -1) AS pickup_courier_sk,
            ISNULL(cd.courier_sk, -1) AS delivery_courier_sk,
            srv.service_sk,
            st.status_sk AS current_status_sk,
            s.weight_gram,
            s.shipping_fee,
            s.cod_amount,
            CAST(DATEDIFF(MINUTE, s.created_at, s.picked_up_at) / 60.0 AS DECIMAL(8,2)) AS pickup_lead_time_hrs,
            CAST(DATEDIFF(MINUTE, s.origin_hub_in_at, s.dest_hub_in_at) / 60.0 AS DECIMAL(8,2)) AS transit_lead_time_hrs,
            CAST(DATEDIFF(MINUTE, s.dest_hub_in_at, s.delivered_at) / 60.0 AS DECIMAL(8,2)) AS last_mile_lead_time_hrs,
            CAST(DATEDIFF(MINUTE, s.created_at, s.delivered_at) / 60.0 AS DECIMAL(8,2)) AS total_lead_time_hrs,
            CASE 
                WHEN s.delivered_at IS NOT NULL AND (DATEDIFF(MINUTE, s.created_at, s.delivered_at) / 60.0) > srv.sla_hours THEN 1
                WHEN s.delivered_at IS NOT NULL THEN 0
                ELSE NULL
            END AS is_sla_breached,
            s.updated_at
        FROM stg.Shipments s
        LEFT JOIN dwh.dim_customer cs ON s.sender_id = cs.customer_nk AND cs.is_current = 1
        LEFT JOIN dwh.dim_customer cr ON s.receiver_id = cr.customer_nk AND cr.is_current = 1
        LEFT JOIN dwh.dim_hub ho ON s.origin_hub_id = ho.hub_nk AND ho.is_current = 1
        LEFT JOIN dwh.dim_hub hd ON s.dest_hub_id = hd.hub_nk AND hd.is_current = 1
        LEFT JOIN dwh.dim_courier cp ON s.pickup_courier_id = cp.courier_nk AND cp.is_current = 1
        LEFT JOIN dwh.dim_courier cd ON s.delivery_courier_id = cd.courier_nk AND cd.is_current = 1
        INNER JOIN dwh.dim_service_type srv ON s.service_id = srv.service_nk
        INNER JOIN dwh.dim_status st ON s.current_status_id = st.status_nk
    ) AS src
    ON target.shipment_nk = src.shipment_id
    WHEN MATCHED THEN
        UPDATE SET
            target.picked_up_date_key = src.picked_up_date_key,
            target.origin_hub_in_date_key = src.origin_hub_in_date_key,
            target.dest_hub_in_date_key = src.dest_hub_in_date_key,
            target.delivered_date_key = src.delivered_date_key,
            target.pickup_courier_sk = src.pickup_courier_sk,
            target.delivery_courier_sk = src.delivery_courier_sk,
            target.current_status_sk = src.current_status_sk,
            target.pickup_lead_time_hrs = src.pickup_lead_time_hrs,
            target.transit_lead_time_hrs = src.transit_lead_time_hrs,
            target.last_mile_lead_time_hrs = src.last_mile_lead_time_hrs,
            target.total_lead_time_hrs = src.total_lead_time_hrs,
            target.is_sla_breached = src.is_sla_breached,
            target.updated_at = src.updated_at
    WHEN NOT MATCHED THEN
        INSERT (
            shipment_nk, waybill_number, created_date_key, picked_up_date_key,
            origin_hub_in_date_key, dest_hub_in_date_key, delivered_date_key,
            sender_customer_sk, receiver_customer_sk, origin_hub_sk, dest_hub_sk,
            pickup_courier_sk, delivery_courier_sk, service_sk, current_status_sk,
            weight_gram, shipping_fee, cod_amount, pickup_lead_time_hrs,
            transit_lead_time_hrs, last_mile_lead_time_hrs, total_lead_time_hrs,
            is_sla_breached, updated_at
        )
        VALUES (
            src.shipment_id, src.waybill_number, src.created_date_key, src.picked_up_date_key,
            src.origin_hub_in_date_key, src.dest_hub_in_date_key, src.delivered_date_key,
            src.sender_customer_sk, src.receiver_customer_sk, src.origin_hub_sk, src.dest_hub_sk,
            src.pickup_courier_sk, src.delivery_courier_sk, src.service_sk, src.current_status_sk,
            src.weight_gram, src.shipping_fee, src.cod_amount, src.pickup_lead_time_hrs,
            src.transit_lead_time_hrs, src.last_mile_lead_time_hrs, src.total_lead_time_hrs,
            src.is_sla_breached, src.updated_at
        );

    -- 3.2. Nạp FACT_TRACKING_EVENTS (Transaction Fact - Append Only)
    INSERT INTO dwh.fact_tracking_events (
        event_nk, shipment_nk, event_date_key, event_time_key,
        current_hub_sk, next_hub_sk, courier_sk, vehicle_sk, status_sk,
        processing_seconds, event_count
    )
    SELECT 
        e.event_id,
        e.shipment_id,
        YEAR(e.event_timestamp)*10000 + MONTH(e.event_timestamp)*100 + DAY(e.event_timestamp) AS event_date_key,
        DATEPART(HOUR, e.event_timestamp)*100 + DATEPART(MINUTE, e.event_timestamp) AS event_time_key,
        ISNULL(hc.hub_sk, -1),
        ISNULL(hn.hub_sk, -1),
        ISNULL(c.courier_sk, -1),
        ISNULL(v.vehicle_sk, -1),
        st.status_sk,
        e.processing_seconds,
        1
    FROM stg.ShipmentTrackingEvents e
    LEFT JOIN dwh.dim_hub hc ON e.current_hub_id = hc.hub_nk AND hc.is_current = 1
    LEFT JOIN dwh.dim_hub hn ON e.next_hub_id = hn.hub_nk AND hn.is_current = 1
    LEFT JOIN dwh.dim_courier c ON e.courier_id = c.courier_nk AND c.is_current = 1
    LEFT JOIN dwh.dim_vehicle v ON e.vehicle_id = v.vehicle_nk
    INNER JOIN dwh.dim_status st ON e.status_id = st.status_nk
    WHERE NOT EXISTS (SELECT 1 FROM dwh.fact_tracking_events f WHERE f.event_nk = e.event_id);

    -- 3.3. Nạp FACT_DAILY_HUB_SNAPSHOT (Periodic Snapshot - Chốt số liệu theo ngày)
    MERGE dwh.fact_daily_hub_snapshot AS target
    USING (
        SELECT 
            f.origin_hub_in_date_key AS snapshot_date_key,
            f.origin_hub_sk AS hub_sk,
            f.service_sk,
            COUNT(f.shipment_sk) AS inbound_cnt,
            SUM(CASE WHEN f.dest_hub_in_date_key > 0 THEN 1 ELSE 0 END) AS outbound_cnt,
            SUM(CASE WHEN f.delivered_date_key > 0 THEN 1 ELSE 0 END) AS delivered_cnt,
            SUM(CASE WHEN f.delivered_date_key = -1 THEN 1 ELSE 0 END) AS backlog_cnt,
            AVG(ISNULL(f.transit_lead_time_hrs, 0) * 3600) AS avg_proc_sec
        FROM dwh.fact_shipment_lifecycle f
        WHERE f.origin_hub_in_date_key > 0
        GROUP BY f.origin_hub_in_date_key, f.origin_hub_sk, f.service_sk
    ) AS src
    ON target.snapshot_date_key = src.snapshot_date_key 
       AND target.hub_sk = src.hub_sk 
       AND target.service_sk = src.service_sk
    WHEN MATCHED THEN
        UPDATE SET
            inbound_packages_count = src.inbound_cnt,
            outbound_packages_count = src.outbound_cnt,
            delivered_packages_count = src.delivered_cnt,
            backlog_packages_count = src.backlog_cnt,
            avg_processing_seconds = src.avg_proc_sec
    WHEN NOT MATCHED THEN
        INSERT VALUES (
            src.snapshot_date_key, src.hub_sk, src.service_sk,
            src.inbound_cnt, src.outbound_cnt, src.delivered_cnt, src.backlog_cnt, src.avg_proc_sec
        );
END;
GO

```

---

### 3.6. Tối ưu hiệu năng: Partitioning & Columnstore Index (`dwh_scripts/04_performance_tuning/`)

**File: `dwh_scripts/04_performance_tuning/indexing_and_partition.sql**`

```sql
USE ExpressDWH;
GO

-- 1. Tạo Partition Function & Partition Scheme theo Quý/Năm cho bảng Fact lớn
IF NOT EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_TrackingDateKey')
BEGIN
    CREATE PARTITION FUNCTION pf_TrackingDateKey (INT)
    AS RANGE RIGHT FOR VALUES (20260101, 20260401, 20260701, 20261001, 20270101);

    CREATE PARTITION SCHEME ps_TrackingDateKey
    AS PARTITION pf_TrackingDateKey ALL TO ([PRIMARY]);
END
GO

-- 2. Tạo Clustered Columnstore Index (CCI) cho bảng Fact_Shipment_Lifecycle
-- Lưu ý: Để tạo CCI, cần tạo trên bảng hoặc Nonclustered Columnstore Index (NCCI) nếu giữ PK
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Fact_Shipment_Lifecycle
ON dwh.fact_shipment_lifecycle (
    created_date_key, delivered_date_key, origin_hub_sk, dest_hub_sk,
    service_sk, current_status_sk, weight_gram, shipping_fee, cod_amount,
    total_lead_time_hrs, is_sla_breached
);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Fact_Tracking_Events
ON dwh.fact_tracking_events (
    event_date_key, event_time_key, current_hub_sk, next_hub_sk,
    courier_sk, vehicle_sk, status_sk, processing_seconds, event_count
);
GO

-- 3. Kịch bản Benchmark đo lường I/O và CPU Time (Dùng để chụp ảnh báo cáo BTL)
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

SELECT 
    h.region,
    h.hub_name,
    s.service_name,
    COUNT(f.shipment_sk) AS total_shipments,
    SUM(f.shipping_fee) AS total_revenue,
    AVG(f.total_lead_time_hrs) AS avg_lead_time_hrs,
    SUM(CAST(f.is_sla_breached AS INT)) * 100.0 / NULLIF(COUNT(f.is_sla_breached), 0) AS sla_breach_pct
FROM dwh.fact_shipment_lifecycle f
INNER JOIN dwh.dim_hub h ON f.origin_hub_sk = h.hub_sk
INNER JOIN dwh.dim_service_type s ON f.service_sk = s.service_sk
GROUP BY h.region, h.hub_name, s.service_name;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;
GO

```

---

### 3.7. Điều phối luồng ETL tự động bằng Apache Airflow (`airflow/dags/express_dwh_etl_dag.py`)

**File: `airflow/dags/express_dwh_etl_dag.py**`

```python
from datetime import datetime, timedelta
import pyodbc
from airflow import DAG
from airflow.operators.python import PythonOperator

CONN_STR_DWH = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=mssql-server,1433;"
    "DATABASE=ExpressDWH;"
    "UID=sa;"
    "PWD=ExpressDWH@2026Strong!;"
    "TrustServerCertificate=yes;"
)

def log_audit(cursor, dag_run_id, step_name, rows_processed, status, error_msg=None):
    cursor.execute("""
        INSERT INTO meta.etl_audit_log (dag_run_id, step_name, rows_processed, status, end_time, error_message)
        VALUES (?, ?, ?, ?, SYSDATETIME(), ?)
    """, (dag_run_id, step_name, rows_processed, status, error_msg))

def extract_oltp_to_staging(**context):
    dag_run_id = context['run_id']
    conn = pyodbc.connect(CONN_STR_DWH)
    cursor = conn.cursor()
    try:
        # 1. Truncate Staging Tables
        for tbl in ['Customers', 'Hubs', 'Couriers', 'Vehicles', 'ServiceTypes', 'Shipments', 'ShipmentTrackingEvents']:
            cursor.execute(f"TRUNCATE TABLE stg.{tbl};")

        # 2. Extract Incremental từ ExpressOLTP sang ExpressDWH.stg dựa vào Watermark
        cursor.execute("""
            INSERT INTO stg.Customers
            SELECT c.customer_id, c.customer_code, c.full_name, c.phone, c.customer_type, c.tier,
                   d.district_name, p.province_name, p.region, c.updated_at
            FROM ExpressOLTP.dbo.Customers c
            JOIN ExpressOLTP.dbo.Districts d ON c.district_id = d.district_id
            JOIN ExpressOLTP.dbo.Provinces p ON d.province_id = p.province_id
            WHERE c.updated_at > (SELECT last_extracted_at FROM meta.etl_watermark WHERE table_name = 'Customers');

            INSERT INTO stg.Hubs
            SELECT h.hub_id, h.hub_code, h.hub_name, h.hub_type, d.district_name, p.province_name,
                   p.region, h.capacity_packages, h.manager_name, h.updated_at
            FROM ExpressOLTP.dbo.Hubs h
            JOIN ExpressOLTP.dbo.Districts d ON h.district_id = d.district_id
            JOIN ExpressOLTP.dbo.Provinces p ON d.province_id = p.province_id
            WHERE h.updated_at > (SELECT last_extracted_at FROM meta.etl_watermark WHERE table_name = 'Hubs');

            INSERT INTO stg.Couriers
            SELECT courier_id, courier_code, full_name, phone, courier_type, base_hub_id, status, updated_at
            FROM ExpressOLTP.dbo.Couriers
            WHERE updated_at > (SELECT last_extracted_at FROM meta.etl_watermark WHERE table_name = 'Couriers');

            INSERT INTO stg.Vehicles
            SELECT vehicle_id, license_plate, vehicle_type, capacity_kg, updated_at
            FROM ExpressOLTP.dbo.Vehicles
            WHERE updated_at > (SELECT last_extracted_at FROM meta.etl_watermark WHERE table_name = 'Vehicles');

            INSERT INTO stg.ServiceTypes
            SELECT service_id, service_code, service_name, transport_mode, sla_hours, base_price, updated_at
            FROM ExpressOLTP.dbo.ServiceTypes
            WHERE updated_at > (SELECT last_extracted_at FROM meta.etl_watermark WHERE table_name = 'ServiceTypes');

            INSERT INTO stg.Shipments
            SELECT shipment_id, waybill_number, sender_id, receiver_id, origin_hub_id, dest_hub_id,
                   service_id, pickup_courier_id, delivery_courier_id, current_status_id,
                   weight_gram, shipping_fee, cod_amount, created_at, picked_up_at,
                   origin_hub_in_at, dest_hub_in_at, delivered_at, updated_at
            FROM ExpressOLTP.dbo.Shipments
            WHERE updated_at > (SELECT last_extracted_at FROM meta.etl_watermark WHERE table_name = 'Shipments');

            INSERT INTO stg.ShipmentTrackingEvents
            SELECT event_id, shipment_id, status_id, current_hub_id, next_hub_id,
                   courier_id, vehicle_id, processing_seconds, event_timestamp
            FROM ExpressOLTP.dbo.ShipmentTrackingEvents
            WHERE event_timestamp > (SELECT last_extracted_at FROM meta.etl_watermark WHERE table_name = 'ShipmentTrackingEvents');
        """)

        cursor.execute("SELECT COUNT(*) FROM stg.Shipments")
        rows = cursor.fetchone()[0]
        log_audit(cursor, dag_run_id, '01_extract_oltp_to_staging', rows, 'SUCCESS')
        conn.commit()
    except Exception as e:
        log_audit(cursor, dag_run_id, '01_extract_oltp_to_staging', 0, 'FAILED', str(e))
        conn.commit()
        raise
    finally:
        conn.close()

def transform_dimensions(**context):
    dag_run_id = context['run_id']
    conn = pyodbc.connect(CONN_STR_DWH)
    cursor = conn.cursor()
    try:
        cursor.execute("EXEC dwh.sp_populate_static_dims;")
        cursor.execute("EXEC dwh.sp_etl_dimensions;")
        log_audit(cursor, dag_run_id, '02_transform_dimensions_scd2', 1, 'SUCCESS')
        conn.commit()
    except Exception as e:
        log_audit(cursor, dag_run_id, '02_transform_dimensions_scd2', 0, 'FAILED', str(e))
        conn.commit()
        raise
    finally:
        conn.close()

def transform_facts_and_update_watermark(**context):
    dag_run_id = context['run_id']
    conn = pyodbc.connect(CONN_STR_DWH)
    cursor = conn.cursor()
    try:
        cursor.execute("EXEC dwh.sp_etl_facts;")
        # Cập nhật High-Watermark sau khi nạp Fact thành công
        cursor.execute("""
            UPDATE meta.etl_watermark SET last_extracted_at = ISNULL((SELECT MAX(updated_at) FROM stg.Customers), last_extracted_at) WHERE table_name = 'Customers';
            UPDATE meta.etl_watermark SET last_extracted_at = ISNULL((SELECT MAX(updated_at) FROM stg.Hubs), last_extracted_at) WHERE table_name = 'Hubs';
            UPDATE meta.etl_watermark SET last_extracted_at = ISNULL((SELECT MAX(updated_at) FROM stg.Couriers), last_extracted_at) WHERE table_name = 'Couriers';
            UPDATE meta.etl_watermark SET last_extracted_at = ISNULL((SELECT MAX(updated_at) FROM stg.Shipments), last_extracted_at) WHERE table_name = 'Shipments';
            UPDATE meta.etl_watermark SET last_extracted_at = ISNULL((SELECT MAX(event_timestamp) FROM stg.ShipmentTrackingEvents), last_extracted_at) WHERE table_name = 'ShipmentTrackingEvents';
        """)
        log_audit(cursor, dag_run_id, '03_transform_facts_and_watermark', 1, 'SUCCESS')
        conn.commit()
    except Exception as e:
        log_audit(cursor, dag_run_id, '03_transform_facts_and_watermark', 0, 'FAILED', str(e))
        conn.commit()
        raise
    finally:
        conn.close()

def data_quality_check(**context):
    dag_run_id = context['run_id']
    conn = pyodbc.connect(CONN_STR_DWH)
    cursor = conn.cursor()
    # Kiểm tra không có vận đơn nào bị âm tiền cước hoặc tổng thời gian giao hàng âm
    cursor.execute("""
        SELECT COUNT(*) FROM dwh.fact_shipment_lifecycle
        WHERE shipping_fee < 0 OR total_lead_time_hrs < 0;
    """)
    invalid_rows = cursor.fetchone()[0]
    if invalid_rows > 0:
        log_audit(cursor, dag_run_id, '04_data_quality_check', invalid_rows, 'FAILED', 'Phát hiện dữ liệu âm bất thường!')
        conn.commit()
        conn.close()
        raise ValueError(f"Data Quality Check Failed: {invalid_rows} dòng vi phạm!")
    log_audit(cursor, dag_run_id, '04_data_quality_check', 0, 'SUCCESS')
    conn.commit()
    conn.close()

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'start_date': datetime(2026, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='express_dwh_etl_dag',
    default_args=default_args,
    schedule_interval='0 */2 * * *', # Chạy định kỳ mỗi 2 tiếng
    catchup=False,
    tags=['express_delivery', 'dwh', 'kimball']
) as dag:

    t1_extract = PythonOperator(
        task_id='extract_oltp_to_staging',
        python_callable=extract_oltp_to_staging
    )

    t2_dims = PythonOperator(
        task_id='transform_dimensions_scd2',
        python_callable=transform_dimensions
    )

    t3_facts = PythonOperator(
        task_id='transform_facts_and_update_watermark',
        python_callable=transform_facts_and_update_watermark
    )

    t4_dq = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check
    )

    t1_extract >> t2_dims >> t3_facts >> t4_dq

```

---

### 3.8. Tầng Semantic Gold & Trực quan hóa Power BI (`bi_analytics/`)

**File: `bi_analytics/01_semantic_views_gold.sql**`

```sql
USE ExpressDWH;
GO

-- View Semantic cung cấp cho Power BI: Phân tích toàn trình Vận đơn & SLA
CREATE OR ALTER VIEW gold.vw_Shipment_SLA_Analytics AS
SELECT 
    f.shipment_sk,
    f.waybill_number AS [Mã Vận Đơn],
    d_create.full_date AS [Ngày Tạo Đơn],
    d_create.month_name AS [Tháng Tạo],
    d_create.year_num AS [Năm],
    d_deliv.full_date AS [Ngày Giao Thành Công],
    c_send.full_name AS [Tên Người Gửi],
    c_send.customer_type AS [Phân Khúc Người Gửi],
    c_send.tier AS [Hạng Khách Hàng],
    h_orig.hub_name AS [Bưu Cục Gửi],
    h_orig.province_name AS [Tỉnh Gửi],
    h_orig.region AS [Miền Gửi],
    h_dest.hub_name AS [Bưu Cục Phát],
    h_dest.province_name AS [Tỉnh Phát],
    h_dest.region AS [Miền Phát],
    emp_deliv.full_name AS [Bưu Tá Giao Hàng],
    srv.service_name AS [Loại Dịch Vụ],
    srv.transport_mode AS [Phương Thức Vận Chuyển],
    srv.sla_hours AS [Cam Kết SLA (Giờ)],
    st.status_name AS [Trạng Thái Hiện Tại],
    st.status_category AS [Nhóm Trạng Thái],
    f.weight_gram / 1000.0 AS [Trọng Lượng (Kg)],
    f.shipping_fee AS [Doanh Thu Cước (VNĐ)],
    f.cod_amount AS [Tiền Thu Hộ COD (VNĐ)],
    f.pickup_lead_time_hrs AS [Thời Gian Lấy Hàng (Giờ)],
    f.transit_lead_time_hrs AS [Thời Gian Trung Chuyển (Giờ)],
    f.last_mile_lead_time_hrs AS [Thời Gian Giao Chặng Cuối (Giờ)],
    f.total_lead_time_hrs AS [Tổng Thời Gian Toàn Trình (Giờ)],
    CASE 
        WHEN f.is_sla_breached = 0 THEN N'Đúng hạn (On-Time)'
        WHEN f.is_sla_breached = 1 THEN N'Trễ hạn (SLA Breached)'
        ELSE N'Đang xử lý'
    END AS [Đánh Giá SLA]
FROM dwh.fact_shipment_lifecycle f
INNER JOIN dwh.dim_date d_create ON f.created_date_key = d_create.date_key
LEFT JOIN dwh.dim_date d_deliv ON f.delivered_date_key = d_deliv.date_key
INNER JOIN dwh.dim_customer c_send ON f.sender_customer_sk = c_send.customer_sk
INNER JOIN dwh.dim_hub h_orig ON f.origin_hub_sk = h_orig.hub_sk
INNER JOIN dwh.dim_hub h_dest ON f.dest_hub_sk = h_dest.hub_sk
LEFT JOIN dwh.dim_courier emp_deliv ON f.delivery_courier_sk = emp_deliv.courier_sk
INNER JOIN dwh.dim_service_type srv ON f.service_sk = srv.service_sk
INNER JOIN dwh.dim_status st ON f.current_status_sk = st.status_sk;
GO

```

**File: `bi_analytics/02_powerbi_dax_measures.dax**`

```dax
// 1. Tổng số lượng vận đơn
Total Shipments = COUNT(fact_shipment_lifecycle[shipment_sk])

// 2. Tổng doanh thu cước vận chuyển
Total Shipping Revenue = SUM(fact_shipment_lifecycle[shipping_fee])

// 3. Tổng giá trị thu hộ COD
Total COD Value = SUM(fact_shipment_lifecycle[cod_amount])

// 4. Số đơn đã giao thành công
Delivered Shipments = 
CALCULATE(
    [Total Shipments],
    dim_status[status_code] = "DELIVERED"
)

// 5. Tỷ lệ giao hàng thành công (Delivery Success Rate %)
Delivery Success Rate % = 
DIVIDE(
    [Delivered Shipments],
    CALCULATE([Total Shipments], dim_status[status_category] IN {"COMPLETED", "EXCEPTION"}),
    0
)

// 6. Tỷ lệ giao hàng đúng cam kết SLA (On-Time Delivery - OTD %)
On-Time Delivery (OTD) % = 
VAR OnTimeCount = 
    CALCULATE(
        [Delivered Shipments],
        fact_shipment_lifecycle[is_sla_breached] = FALSE()
    )
RETURN
    DIVIDE(OnTimeCount, [Delivered Shipments], 0)

// 7. Thời gian toàn trình trung bình (Giờ)
Avg Total Lead Time (Hrs) = AVERAGE(fact_shipment_lifecycle[total_lead_time_hrs])

// 8. Tỷ lệ chiếm dụng công suất Bưu cục (Hub Utilization %)
Hub Capacity Utilization % = 
DIVIDE(
    SUM(fact_daily_hub_snapshot[inbound_packages_count]) + SUM(fact_daily_hub_snapshot[backlog_packages_count]),
    SUM(dim_hub[capacity_packages]),
    0
)

```

**File: `bi_analytics/03_dashboard_wireframe.md**`

```markdown
# ĐẶC TẢ 3 TRANG BÁO CÁO POWER BI (EXPRESS DELIVERY ANALYTICS)

## Trang 1: Executive Overview (Tổng quan Điều hành & Doanh thu)
- **4 KPI Cards trên cùng:** `Total Shipments`, `Total Shipping Revenue`, `Total COD Value`, `On-Time Delivery (OTD) %`.
- **Line & Clustered Column Chart:** Biến động số lượng đơn hàng (Cột) và Doanh thu cước (Đường) theo Ngày/Tháng (`dim_date`).
- **Donut Chart:** Tỷ trọng doanh thu theo Loại dịch vụ (`Hỏa tốc`, `Nhanh`, `Tiết kiệm`) và Phân khúc khách hàng (`B2B_SHOP` vs `B2C_RETAIL`).
- **Matrix Table:** Top 10 Shop B2B mang lại doanh thu cao nhất kèm trạng thái Hạng khách hàng (`Tier` từ SCD Type 2).

## Trang 2: SLA & End-to-End Lead Time Analysis (Phân tích Thời gian toàn trình)
- **Stacked Bar Chart (Decomposition of Lead Time):** Phân rã thời gian trung bình của 3 chặng: `Pickup Lead Time` (Lấy hàng) + `Transit Lead Time` (Trung chuyển đường trục) + `Last-Mile Lead Time` (Giao hàng chặng cuối) theo từng tuyến Liên miền (`Miền Bắc -> Miền Nam`).
- **Gauge Chart:** Tỷ lệ `On-Time Delivery (OTD) %` so với mục tiêu SLA toàn công ty (Target = 95%).
- **Scatter Plot:** Tương quan giữa Khối lượng đơn hàng và Tỷ lệ trễ SLA theo từng Bưu cục phát.

## Trang 3: Hub Bottleneck & Fleet Operations (Giám sát Tắc nghẽn Bưu cục & Đội xe)
- **Heatmap / Matrix:** Số lượng kiện hàng quét mã vạch (`fact_tracking_events`) theo Khung giờ trong ngày (`dim_time[shift_name]`) và Loại Bưu cục (`MEGA_HUB`, `SORTING_CENTER`).
- **Clustered Bar Chart:** Top 5 Bưu cục có lượng hàng tồn đọng (`backlog_packages_count` từ `fact_daily_hub_snapshot`) cao nhất cần tăng cường xe tải đường trục (`LINEHAUL_TRUCK`).

```

---

## PHẦN 4: HƯỚNG DẪN CHẠY THỬ NGHIỆM (QUICK START TRÊN POWERSHELL)

Thực hiện tuần tự 4 bước sau trên PowerShell để dựng toàn bộ hệ thống:

1. **Khởi động Docker Containers:**

```powershell
docker-compose up -d

```

2. **Khởi tạo DB Nguồn OLTP & Kho dữ liệu DWH:**

```powershell
docker exec -it express_mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "ExpressDWH@2026Strong!" -C -i /scripts/oltp_source/ddl_create_oltp.sql
docker exec -it express_mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "ExpressDWH@2026Strong!" -C -i /scripts/dwh_scripts/01_staging_bronze/ddl_staging_and_audit.sql
docker exec -it express_mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "ExpressDWH@2026Strong!" -C -i /scripts/dwh_scripts/02_core_silver_gold/ddl_star_schema.sql
docker exec -it express_mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "ExpressDWH@2026Strong!" -C -i /scripts/dwh_scripts/03_stored_procedures_etl/sp_etl_master.sql
docker exec -it express_mssql /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "ExpressDWH@2026Strong!" -C -i /scripts/dwh_scripts/04_performance_tuning/indexing_and_partition.sql

```

3. **Chạy Script sinh dữ liệu giả lập OLTP (500 vận đơn & ~2.500 sự kiện quét mã vạch):**

```powershell
python ./oltp_source/generate_fake_data.py

```

4. **Kích hoạt DAG trên giao diện Airflow:**

* Truy cập `http://localhost:8080` (Tài khoản: `admin` / `admin`).
* Bật công tắc (Unpause) và nhấn **Trigger DAG** cho `express_dwh_etl_dag`.
* Mở Power BI Desktop $\rightarrow$ Chọn **SQL Server** $\rightarrow$ Nhập `localhost,1433`, Database `ExpressDWH` $\rightarrow$ Chọn các bảng trong schema `dwh` hoặc View `gold.vw_Shipment_SLA_Analytics` để dựng Dashboard.
