{{ config(unique_key='settlement_id') }}

WITH src AS (
    SELECT
        c.settlement_id,
        c.shipment_id,
        c.cod_amount,
        TRIM(c.settlement_status) AS settlement_status,
        c.settled_at,
        c.updated_at,
        c.batch_id
    FROM {{ source('stg', 'cod_settlements') }} c
    WHERE c.batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.settlement_id ORDER BY c.updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.settlement_id = tgt.settlement_id
WHERE tgt.settlement_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}