### Các lỗi cốt lõi phát hiện được

1. **dbt Snapshot SCD2 không hỗ trợ đọc trực tiếp CTE hoặc `ref()` có logic phức tạp mà thiếu config alias / unique key chuẩn**

* File `snap_customers.sql` và `snap_couriers.sql` dùng `target_schema='SILVER'` nhưng không chỉ định rõ `updated_at` làm timestamp phụ trợ, gây lỗi tracking nếu `check_cols=['row_hash']` không nhận diện được cột thời điểm thay đổi.

2. **Hàm `SEQ4()` trong Snowflake Generator (`dim_date.sql`) không liên tục và không đơn điệu**

* Snowflake khuyến cáo rõ ràng: `SEQ4()` chỉ sinh số duy nhất trong vi mô worker, **không đảm bảo liên tục (0, 1, 2, 3...) khi sinh lượng lớn bản ghi**. Điều này làm `dim_date` bị nhảy cóc ngày hoặc trùng ngày. Giải pháp chuẩn là dùng `ROW_NUMBER() OVER (ORDER BY NULL) - 1`.

3. **Cú pháp dbt test `accepted_values` trong `schema.yml**`

* `accepted_values` trong dbt Core yêu cầu danh sách dạng YAML sequence list (dùng dấu gạch đầu dòng `-`), việc để dạng mảng inline `values: ['CREATED', ...]` dễ gây lỗi parse ở một số version `dbt-core`.

4. **Xung đột kiểu dữ liệu Timestamp giữa Pandas, SQLAlchemy và Snowflake `write_pandas**`

* Khi `df[col] = formatted.where(...)` được đẩy qua `write_pandas`, các cột được gán chuỗi sẽ bị Pandas coi là `object` (chuỗi), trong khi DDL Snowflake là `TIMESTAMP_NTZ`. Nếu không ép kiểu chuẩn qua `pd.to_datetime(..., errors='coerce')`, lệnh copy ngầm của Snowflake sẽ bắn lỗi `Numeric/Date value out of range` hoặc `Timestamp format mismatch`.

5. **Airflow Docker Compose xung đột mount thư mục và `requirements.txt**`

* Trong Dockerfile/Compose, việc mount đè toàn bộ thư mục cha và chạy `pip install` ở entrypoint mỗi lần khởi động container rất chậm và dễ gây deadlock file lock.

6. **dbt Incremental Strategy trên Snowflake**

* Khi dùng `incremental_strategy='merge'` trên Snowflake với dbt, bắt buộc phải có `unique_key`. `fact_daily_hub_snapshot` dùng `unique_key=['snapshot_date_key', 'hub_sk']` cần đảm bảo dbt adapter xử lý composite key chuẩn.

---

### Chi tiết các file cần sửa đổi

#### 1. Sửa `scripts/extract_load_snowflake.py` (Xử lý chuẩn Datetime cho `write_pandas`)

Thay thế đoạn format chuỗi bằng việc chuẩn hóa Timestamp về đúng timezone `None` (NTZ) để `snowflake.connector.pandas_tools.write_pandas` tự động map sang kiểu `TIMESTAMP_NTZ` trong Snowflake mà không bị lỗi kiểu chuỗi:

```python
# scripts/extract_load_snowflake.py (Đoạn xử lý dataframe trước khi write_pandas)

                if row_count > 0:
                    df["batch_id"] = batch_id
                    df["extracted_at"] = pd.Timestamp(snapshot_ts)
                    df.columns = [c.upper() for c in df.columns]

                    # Chuyển đổi an toàn sang Timestamp không timezone (TIMESTAMP_NTZ) cho Snowflake
                    for col in df.columns:
                        if "DATE" in col or "TIME" in col or col in ["CREATED_AT", "UPDATED_AT", "EXTRACTED_AT"]:
                            df[col] = pd.to_datetime(df[col], errors="coerce").dt.tz_localize(None)

                    write_pandas(
                        conn=sf_conn,
                        df=df,
                        table_name=table.upper(),
                        schema="STG",
                        auto_create_table=False,
                        quote_identifiers=False,
                    )
```

---

#### 2. Sửa `dbt_project/snapshots/` (Cấu hình chuẩn Snowflake dbt Snapshot)

Trong Snowflake, snapshot dbt cần chỉ định rõ `updated_at` để dbt tính toán chính xác metadata nội bộ `dbt_valid_from` và `dbt_valid_to`:

##### File: `dbt_project/snapshots/snap_customers.sql`

```sql
{% snapshot snap_customers %}

{{
    config(
      target_schema='SILVER',
      unique_key='customer_id',
      strategy='check',
      check_cols=['row_hash'],
      updated_at='updated_at',
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

##### File: `dbt_project/snapshots/snap_couriers.sql`

```sql
{% snapshot snap_couriers %}

{{
    config(
      target_schema='SILVER',
      unique_key='courier_id',
      strategy='check',
      check_cols=['row_hash'],
      updated_at='updated_at',
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

#### 3. Sửa `dbt_project/models/gold/dimensions/dim_date.sql` (Khắc phục bug `SEQ4()` của Snowflake)

Dùng `ROW_NUMBER() OVER (ORDER BY NULL) - 1` để đảm bảo chuỗi ngày tăng liên tục từ `2024-01-01`:

```sql
WITH date_spine AS (
    SELECT 
        DATEADD('day', (ROW_NUMBER() OVER (ORDER BY NULL) - 1), '2024-01-01'::DATE) AS full_date
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

---

#### 4. Sửa `dbt_project/models/schema.yml` (Chuẩn hóa cấu hình Test)

Định dạng lại `accepted_values` thành format YAML list chuẩn để tránh warning/error từ dbt Core:

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
              expression: "> 0"
      - name: current_status
        tests:
          - accepted_values:
              values:
                - 'CREATED'
                - 'PICKED_UP'
                - 'IN_TRANSIT'
                - 'DELIVERING'
                - 'DELIVERY_FAILED'
                - 'DELIVERED'
                - 'RETURNED'
                - 'CANCELLED'

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

#### 5. Sửa `dags/express_delivery_snowflake_dbt_dag.py` (Khắc phục lỗi Escape JSON trên BashOperator)

Khi truyền `--vars` chứa chuỗi JSON qua BashOperator của Airflow, việc lồng dấu nháy kép `"` và nháy đơn `'` rất dễ bị shell nuốt ký tự dẫn đến syntax error của dbt. Dưới đây là cách truyền an toàn:

```python
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command="""
            BATCH_ID="{{ ti.xcom_pull(task_ids='extract_postgres_to_snowflake_stg', key='batch_id') }}"
            dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project \
                    --select silver --vars "{\"batch_id\": \"$BATCH_ID\"}"
        """,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot_scd2",
        bash_command="""
            BATCH_ID="{{ ti.xcom_pull(task_ids='extract_postgres_to_snowflake_stg', key='batch_id') }}"
            dbt snapshot --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project \
                         --vars "{\"batch_id\": \"$BATCH_ID\"}"
        """,
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold_marts",
        bash_command="""
            BATCH_ID="{{ ti.xcom_pull(task_ids='extract_postgres_to_snowflake_stg', key='batch_id') }}"
            dbt run --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project \
                    --select gold.dimensions gold.facts gold.analytics \
                    --vars "{\"batch_id\": \"$BATCH_ID\"}"
        """,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command="""
            BATCH_ID="{{ ti.xcom_pull(task_ids='extract_postgres_to_snowflake_stg', key='batch_id') }}"
            dbt test --project-dir /opt/airflow/dbt_project --profiles-dir /opt/airflow/dbt_project \
                     --vars "{\"batch_id\": \"$BATCH_ID\"}"
        """,
    )
```

- cập nhật thư viện requirements
