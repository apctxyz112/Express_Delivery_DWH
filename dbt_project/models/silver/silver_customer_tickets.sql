{{ config(unique_key='ticket_id') }}

WITH src AS (
    SELECT
        t.ticket_id,
        t.shipment_id,
        t.customer_id,
        TRIM(t.issue_type) AS issue_type,
        TRIM(t.ticket_status) AS ticket_status,
        t.created_at,
        t.updated_at,
        t.batch_id
    FROM {{ source('stg', 'customer_tickets') }} t
    WHERE t.batch_id = '{{ var("batch_id") }}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.ticket_id ORDER BY t.updated_at DESC) = 1
)
SELECT s.*
FROM src s
{% if is_incremental() %}
LEFT JOIN {{ this }} tgt ON s.ticket_id = tgt.ticket_id
WHERE tgt.ticket_id IS NULL OR tgt.updated_at <= s.updated_at
{% endif %}