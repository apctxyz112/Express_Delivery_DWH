{{ config(unique_key='hub_id') }}

WITH src AS (
    SELECT
        hub_id,
        TRIM(hub_code) AS hub_code,
        TRIM(hub_name) AS hub_name,
        TRIM(hub_level) AS hub_level,
        TRIM(city) AS city,
        TRIM(district) AS district,
        capacity,
        updated_at,
        batch_id
    FROM {{ source('stg', 'hubs') }}
    WHERE batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hub_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.hub_id = tgt.hub_id
WHERE tgt.hub_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}