
  
    

        create or replace transient table EXPRESS_DWH.GOLD.dim_courier
         as
        (WITH ordered_versions AS (
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
    FROM EXPRESS_DWH.SILVER.snap_couriers c
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
LEFT JOIN EXPRESS_DWH.GOLD.dim_hub dh ON w.hub_id = dh.hub_sk
LEFT JOIN EXPRESS_DWH.GOLD.dim_vehicle dv ON w.vehicle_id = dv.vehicle_sk
        );
      
  