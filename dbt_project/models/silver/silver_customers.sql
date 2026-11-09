{{ config(unique_key='customer_id') }}

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
    FROM {{ source('stg', 'customers') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.customer_id = tgt.customer_id
WHERE tgt.customer_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}