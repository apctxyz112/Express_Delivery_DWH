{{ config(unique_key='service_type_id') }}

WITH src AS (
    SELECT
        service_type_id,
        TRIM(service_code) AS service_code,
        TRIM(service_name) AS service_name,
        sla_hours,
        is_express,
        updated_at,
        batch_id
    FROM {{ source('stg', 'service_types') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service_type_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.service_type_id = tgt.service_type_id
WHERE tgt.service_type_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}