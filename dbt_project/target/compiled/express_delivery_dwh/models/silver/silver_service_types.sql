

WITH src AS (
    SELECT
        service_type_id,
        TRIM(service_code) AS service_code,
        TRIM(service_name) AS service_name,
        sla_hours,
        is_express,
        updated_at,
        batch_id
    FROM EXPRESS_DWH.STG.service_types
    WHERE batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY service_type_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
