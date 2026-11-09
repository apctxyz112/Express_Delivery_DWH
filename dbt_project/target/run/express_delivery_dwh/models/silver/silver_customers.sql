
  
    

        create or replace transient table EXPRESS_DWH.SILVER.silver_customers
         as
        (

WITH src AS (
    SELECT
        customer_id,
        TRIM(customer_code) AS customer_code,
        TRIM(customer_name) AS customer_name,
        TRIM(customer_type) AS customer_type,
        TRIM(membership_tier) AS membership_tier,
        -- [AUDIT-B6] Không bịa số '0000000000'; chỉ giữ số hợp lệ 9-12 chữ số, còn lại gán NULL
        CASE
            WHEN REGEXP_LIKE(REGEXP_REPLACE(COALESCE(phone_number, ''), '[^0-9]', ''), '^[0-9]{9,12}$')
            THEN REGEXP_REPLACE(phone_number, '[^0-9]', '')
            ELSE NULL
        END AS phone_number,
        TRIM(city) AS city,
        TRIM(district) AS district,
        MD5(
            COALESCE(TRIM(customer_name), '') || '|' ||
            COALESCE(TRIM(customer_type), '') || '|' ||
            COALESCE(TRIM(membership_tier), '') || '|' ||
            COALESCE(TRIM(city), '') || '|' ||
            COALESCE(TRIM(district), '')
        ) AS row_hash,
        updated_at,
        batch_id
    FROM EXPRESS_DWH.STG.customers
    WHERE batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s

        );
      
  