
Chuyển đổi từ mô hình truyền thống (chạy ETL thuần trên Python/PostgreSQL) sang **Modern Data Stack (MDS)** với **Airflow + Snowflake + dbt** sẽ chuyển hẳn kiến trúc từ **ETL sang ELT**. Lúc này:

* **Airflow + Python/Ingestion Tool** chỉ làm nhiệm vụ **EL** (Extract từ OLTP và Load thô vào Snowflake Staging).
* **Snowflake** đóng vai trò Data Cloud lưu trữ và cung cấp năng lượng tính toán (Compute / Virtual Warehouse).
* **dbt (data build tool)** đảm nhận phần **T** (Transform) toàn bộ các tầng `Silver`, `Gold` (Facts/Dims), quản lý SCD Type 2 qua `dbt snapshot`, kiểm tra Data Quality qua `dbt test`, và tự động build Semantic/Materialized Views.

---

### 1. Kiến trúc luồng chạy mới (Modern ELT Pipeline)

```text
 ┌────────────────┐
 │   PostgreSQL   │ (OLTP Source: Customers, Hubs, Shipments, Events...)
 └───────┬────────┘
         │
         │  [E - EXTRACT] Airflow Python Operator / CDC
         ▼
 ┌────────────────┐
 │   Stage / S3   │ (Tạm thời: Parquet/CSV files trên Internal Stage hoặc AWS S3)
 └───────┬────────┘
         │
         │  [L - LOAD] COPY INTO (Idempotent Raw Load)
         ▼
 ┌─────────────────────────────────────────────────────────────────────────────┐
 │                         SNOWFLAKE DATA CLOUD                                │
 │                                                                             │
 │  ┌───────────────────────────────────────────────────────────────────────┐  │
 │  │ 🥉 BRONZE / RAW (DB: RAW_DB, Schema: STG)                             │  │
 │  │   • Raw tables lưu dữ liệu trích xuất dạng append/truncate            │  │
 │  │   • Cột metadata: _loaded_at, _file_name, batch_id                     │  │
 │  └───────────────────────────────────┬───────────────────────────────────┘  │
 │                                      │                                      │
 │                                      │  dbt test (Freshness / Not Null)     │
 │                                      ▼                                      │
 │  ┌───────────────────────────────────────────────────────────────────────┐  │
 │  │ 🥈 SILVER / CLEANSED (DB: ANALYTICS_DB, Schema: SILVER)                │  │
 │  │   • dbt models: Type casting, Data cleansing, Loại bỏ rác             │  │
 │  │   • Data Quality Gate & Quarantine: dbt test severity 'warn'/'error'  │  │
 │  │   • dbt Snapshots: SCD Type 2 tự động cho Dim_Customer & Dim_Courier │  │
 │  └───────────────────────────────────┬───────────────────────────────────┘  │
 │                                      │                                      │
 │                                      │  dbt run (Incremental / Dynamic Table│
 │                                      ▼                                      │
 │  ┌───────────────────────────────────────────────────────────────────────┐  │
 │  │ 🥇 GOLD / KIMBALL MARTS (DB: ANALYTICS_DB, Schema: GOLD)              │  │
 │  │   • 8 Dimensions (Dim_Date, Dim_Hub, Dim_Courier SCD2, Dim_Customer)  │  │
 │  │   • 3 Facts (Fact_Shipment_Lifecycle, Fact_Tracking_Events, Snapshot) │  │
 │  │   • Semantic Layer / Dynamic Tables: Dashboard Views & Metrics        │  │
 │  └───────────────────────────────────┬───────────────────────────────────┘  │
 └──────────────────────────────────────┼──────────────────────────────────────┘
                                        │
                                        ▼
                     ┌─────────────────────────────────────┐
                     │   BI / Consumers (Metabase, Tableau)│
                     └─────────────────────────────────────┘
```

---

### 2. Thiết kế chi tiết từng cấu phần trong dbt & Snowflake

#### A. Airflow: Chỉ còn làm Bộ điều phối (Orchestration)

Airflow không còn chạy các script SQL biến đổi phức tạp, mà tận dụng `Cosmos` (hoặc `BashOperator` / `DbtTaskGroup`) để trigger dbt:

1. **Task 1 (`extract_and_load_raw`)**: Trích xuất dữ liệu mới từ PostgreSQL OLTP đẩy lên Snowflake Stage, sau đó thực hiện lệnh `COPY INTO raw_db.stg.<table_name>`.
2. **Task 2 (`dbt_source_test`)**: Kiểm tra freshness và schema của dữ liệu thô.
3. **Task 3 (`dbt_snapshot`)**: Chạy lệnh snapshot để cập nhật SCD Type 2 vào các bảng chiều.
4. **Task 4 (`dbt_run_silver`)**: Làm sạch, chuẩn hóa, ép kiểu, gắn thẻ flag quarantine.
5. **Task 5 (`dbt_run_gold`)**: Tính toán Facts và Dimensions (sử dụng cơ chế `incremental` của dbt).
6. **Task 6 (`dbt_test_marts`)**: Kiểm tra tính toàn vẹn (Unique, Foreign Key relationships, Custom business rules).

---

#### B. Cách dbt giải quyết hoàn toàn các vấn đề phức tạp ở bản cũ

| Bài toán ở bản cũ

 | Xử lý bằng Code Python/SQL cũ

| **Cách xử lý tối ưu với dbt + Snowflake**        |  |  |
| ------------------------------------------------------------ | - | - |
| **SCD Type 2** (Khách đổi hạng, Shipper đổi Hub) |  |  |

 | Tự viết CTE so sánh `row_hash`, tự đóng `valid_to`, mở bản ghi mới

 | Sử dụng **`dbt snapshot`** với chiến lược `check` (danh sách cột) hoặc `timestamp` (dựa trên `updated_at`). dbt tự sinh `dbt_valid_from`, `dbt_valid_to`, `dbt_is_current`. |
| **Incremental Load** (Accumulating Fact)

 | Tự duy trì Watermark, lookback 10 phút, `ON CONFLICT DO UPDATE`

 | Sử dụng materialization **`incremental`** với cấu hình: `unique_key='shipment_id_nk'` và `incremental_strategy='merge'`. Snowflake tự động tối ưu hóa lệnh `MERGE INTO`. |
| **Data Quality Gate & Quarantine**

 | Script INSERT vào bảng `meta.data_quarantine`

 | Khai báo các bài test trong file `.yml` (Not null, Accepted values, Expression tests). dbt cho phép cấu hình `store_failures = true` để tự động lưu toàn bộ các dòng vi phạm vào schema `dbt_test__audit` (hoạt động hệt như Quarantine Table). |
| **Materialized Views** (Hiệu năng Hub & Shipper)

 | Thủ công gọi `REFRESH MATERIALIZED VIEW` ở bước cuối

 | Sử dụng tính năng **Snowflake Dynamic Tables** (hoặc `materialized='materializedview'` trong dbt) để Snowflake tự động refresh liên tục theo lịch định sẵn (`target_lag = '1 hour'`). |

---

### 3. Minh họa Code dbt mẫu cho dự án

#### File 1: Cấu hình SCD Type 2 tự động (`snapshots/snap_couriers.sql`)

Thay vì viết câu lệnh SQL phức tạp nhiều bước, dbt chỉ cần một file snapshot:

```sql
{% snapshot snap_couriers %}

{{
    config(
      target_schema='silver',
      strategy='check',
      unique_key='courier_id',
      check_cols=['courier_name', 'hub_id', 'contract_type', 'vehicle_id'],
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
    updated_at
FROM {{ source('raw_staging', 'couriers') }}

{% endsnapshot %}

```

#### File 2: Nạp Fact Accumulating Incremental (`models/gold/fact_shipment_lifecycle.sql`)

Áp dụng chiến lược Merge tự động của Snowflake:

```sql
{{
    config(
        materialized='incremental',
        unique_key='shipment_id_nk',
        incremental_strategy='merge',
        cluster_by=['order_date_key', 'origin_hub_sk']
    )
}}

WITH silver_shipments AS (
    SELECT * FROM {{ ref('silver_shipments') }}
    {% if is_incremental() %}
        -- Tự động filter chỉ lấy đơn có cập nhật mới (kèm lookback 10 phút)
        WHERE updated_at >= (SELECT COALESCE(MAX(updated_at), '1970-01-01') - INTERVAL '10 minutes' FROM {{ this }})
    {% endif %}
),

events_summary AS (
    SELECT
        shipment_id,
        MIN(CASE WHEN event_type = 'INBOUND_SCAN' THEN event_time END) AS first_sorting_time,
        MAX(CASE WHEN event_type = 'OUTBOUND_SCAN' THEN event_time END) AS last_sorting_time,
        COUNT(CASE WHEN event_type IN ('DELIVERY_ATTEMPT', 'DELIVERED') THEN 1 END) AS delivery_attempt_count
    FROM {{ ref('silver_tracking_events') }}
    GROUP BY shipment_id
)

SELECT
    s.shipment_id AS shipment_id_nk,
    s.shipment_number,
    COALESCE(c.customer_sk, -1) AS sender_customer_sk,
    COALESCE(h_orig.hub_sk, -1) AS origin_hub_sk,
    COALESCE(h_dest.hub_sk, -1) AS destination_hub_sk,
  
    TO_NUMBER(TO_VARCHAR(s.order_date, 'YYYYMMDD')) AS order_date_key,
    s.order_date AS order_timestamp,
    ev.first_sorting_time AS first_sorting_timestamp,
    s.actual_delivery_date AS final_delivery_timestamp,
  
    s.weight,
    s.freight_charge,
    s.cod_amount,
    COALESCE(ev.delivery_attempt_count, 0) AS delivery_attempt_count,
  
    (s.current_status = 'DELIVERED') AS is_delivered,
    (s.current_status = 'DELIVERED' AND s.actual_delivery_date <= s.promised_delivery_date) AS is_on_time,
  
    CURRENT_TIMESTAMP() AS dwh_updated_at
FROM silver_shipments s
LEFT JOIN events_summary ev ON s.shipment_id = ev.shipment_id
LEFT JOIN {{ ref('dim_hub') }} h_orig ON s.origin_hub_id = h_orig.hub_id
LEFT JOIN {{ ref('dim_hub') }} h_dest ON s.destination_hub_id = h_dest.hub_id
LEFT JOIN {{ ref('snap_customers') }} c 
  ON s.sender_customer_id = c.customer_id
 AND s.order_date >= c.dbt_valid_from 
 AND s.order_date < COALESCE(c.dbt_valid_to, '9999-12-31')

```

#### File 3: Cấu hình Data Quality & Quarantine (`models/schema.yml`)

Thay cho hàng loạt câu query INSERT vào bảng `data_quarantine`:

```yaml
version: 2

models:
  - name: silver_shipments
    description: "Dữ liệu vận đơn đã qua lọc sạch"
    columns:
      - name: shipment_number
        tests:
          - not_null:
              config:
                severity: error
                store_failures: true # Tự động tạo bảng lưu dòng lỗi (Quarantine)
          - unique:
              config:
                severity: error
                store_failures: true

      - name: weight
        tests:
          - dbt_utils.expression_is_true:
              arguments:
                expression: "> 0"
              config:
                severity: error
                store_failures: true

      - name: current_status
        tests:
          - accepted_values:
              values: ['CREATED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERING', 'DELIVERY_FAILED', 'DELIVERED', 'RETURNED', 'CANCELLED']
              config:
                severity: warn
                store_failures: true

```

---

### 4. DAG Airflow mới với Cosmos (`dags/dbt_snowflake_dwh_dag.py`)

Khi dùng dbt, Airflow DAG gọn nhẹ và minh bạch hơn nhiều:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig

default_args = {
    "owner": "express_dwh_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="express_delivery_snowflake_elt",
    default_args=default_args,
    schedule="0 */4 * * *",
    catchup=False,
    max_active_runs=1,
) as dag:

    # 1. Trích xuất Postgres -> Đẩy vào Snowflake STG
    extract_and_load_raw = PythonOperator(
        task_id="extract_postgres_to_snowflake_stage",
        python_callable=lambda: print("Extracting OLTP tables and COPY INTO raw_db.stg..."),
    )

    # 2. Cosmos tự động đọc dbt project và render ra cây task DAG
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform_pipeline",
        project_config=ProjectConfig("/opt/airflow/dbt_project"),
        profile_config=ProfileConfig(
            profile_name="express_snowflake",
            target_name="prod",
            profiles_yml_filepath="/opt/airflow/dbt_project/profiles.yml",
        ),
    )

    extract_and_load_raw >> dbt_transform

```

---

### 5. Ưu điểm vượt trội so với giải pháp ban đầu

1. **Khả năng mở rộng (Scalability)**: Toàn bộ quá trình biến đổi (Transform) đẩy xuống Snowflake thực thi theo chuẩn song song MPP, loại bỏ hoàn toàn hiện tượng nghẽn RAM/CPU trên container Airflow hoặc máy chủ chứa script Python.
2. **Loại bỏ nguy cơ Deadlock logic & Script DDL dài**: dbt tự động quản lý các transaction `CREATE`, `REPLACE`, `MERGE` an toàn, không còn lỗi xung đột transaction do chạy thủ công nhiều lệnh SQL.
3. **Data Lineage & Documentation tự động**: Chạy lệnh `dbt docs generate` sẽ xuất trực tiếp sơ đồ quan hệ giữa các bảng từ Staging đến Marts kèm tài liệu từng trường dữ liệu cho cả team Data Analytics.
