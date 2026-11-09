{{ config(unique_key='courier_id') }}

WITH src AS (
    SELECT
        courier_id,
        TRIM(courier_code) AS courier_code,
        TRIM(courier_name) AS courier_name,
        hub_id,
        TRIM(contract_type) AS contract_type,
        vehicle_id,
        effective_date,
        MD5(
            COALESCE(TRIM(courier_name), '') || '|' ||
            COALESCE(TO_VARCHAR(hub_id), '-1') || '|' ||
            COALESCE(TRIM(contract_type), '') || '|' ||
            COALESCE(TO_VARCHAR(vehicle_id), '-1')
        ) AS row_hash,
        updated_at,
        batch_id
    FROM {{ source('stg', 'couriers') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY courier_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.courier_id = tgt.courier_id
WHERE tgt.courier_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}