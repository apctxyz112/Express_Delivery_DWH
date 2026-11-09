{{ config(unique_key='vehicle_id') }}

WITH src AS (
    SELECT
        vehicle_id,
        TRIM(vehicle_code) AS vehicle_code,
        TRIM(vehicle_type) AS vehicle_type,
        capacity_kg,
        updated_at,
        batch_id
    FROM {{ source('stg', 'vehicles') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.vehicle_id = tgt.vehicle_id
WHERE tgt.vehicle_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}