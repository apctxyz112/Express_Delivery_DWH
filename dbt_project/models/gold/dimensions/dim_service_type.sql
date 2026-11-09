SELECT
    -1 AS service_type_sk,
    'UNKNOWN' AS service_type_nk,
    'Unknown Service' AS service_name,
    0 AS sla_hours,
    FALSE AS is_express
UNION ALL
SELECT
    service_type_id AS service_type_sk,
    service_code AS service_type_nk,
    service_name,
    sla_hours,
    is_express
FROM {{ ref('silver_service_types') }}