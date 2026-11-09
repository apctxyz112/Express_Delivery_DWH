{{ config(unique_key='payment_method_id') }}

WITH src AS (
    SELECT
        payment_method_id,
        TRIM(payment_method_code) AS payment_method_code,
        TRIM(payment_method_name) AS payment_method_name,
        TRIM(payer_type) AS payer_type,
        has_cod,
        updated_at,
        batch_id
    FROM {{ source('stg', 'payment_methods') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_method_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.payment_method_id = tgt.payment_method_id
WHERE tgt.payment_method_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}