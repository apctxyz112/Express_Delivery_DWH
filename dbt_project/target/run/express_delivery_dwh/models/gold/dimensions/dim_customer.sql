
  
    

        create or replace transient table EXPRESS_DWH.GOLD.dim_customer
         as
        (WITH ordered_versions AS (
    SELECT
        customer_id,
        customer_code AS customer_nk,
        customer_name,
        customer_type,
        membership_tier,
        city,
        district,
        row_hash,
        updated_at,
        dbt_valid_from,
        dbt_valid_to,
        ROW_NUMBER() OVER (PARTITION BY customer_code ORDER BY dbt_valid_from, updated_at) AS ver_num,
        LEAD(updated_at) OVER (PARTITION BY customer_code ORDER BY dbt_valid_from, updated_at) AS next_updated_at
    FROM EXPRESS_DWH.SILVER.snap_customers
),
resolved_bounds AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY dbt_valid_from, customer_id, ver_num) AS customer_sk,
        customer_nk,
        customer_name,
        customer_type,
        membership_tier,
        city,
        district,
        IFF(ver_num = 1, '1970-01-01 00:00:00'::TIMESTAMP_NTZ, updated_at) AS valid_from,
        COALESCE(next_updated_at, '9999-12-31 23:59:59'::TIMESTAMP_NTZ) AS valid_to,
        IFF(next_updated_at IS NULL AND dbt_valid_to IS NULL, TRUE, FALSE) AS is_current,
        row_hash
    FROM ordered_versions
)
SELECT
    -1 AS customer_sk,
    'UNKNOWN' AS customer_nk,
    'Unknown Customer' AS customer_name,
    'Unknown' AS customer_type,
    'Unknown' AS membership_tier,
    'Unknown' AS city,
    'Unknown' AS district,
    '1900-01-01 00:00:00'::TIMESTAMP_NTZ AS valid_from,
    '9999-12-31 23:59:59'::TIMESTAMP_NTZ AS valid_to,
    TRUE AS is_current,
    'UNKNOWN' AS row_hash
UNION ALL
SELECT * FROM resolved_bounds
        );
      
  