{{ config(unique_key='status_reason_id') }}

WITH src AS (
    SELECT
        status_reason_id,
        TRIM(status_code) AS status_code,
        TRIM(status_name) AS status_name,
        TRIM(reason_code) AS reason_code,
        TRIM(reason_name) AS reason_name,
        TRIM(event_category) AS event_category,
        updated_at,
        batch_id
    FROM {{ source('stg', 'status_reasons') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status_reason_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.status_reason_id = tgt.status_reason_id
WHERE tgt.status_reason_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}