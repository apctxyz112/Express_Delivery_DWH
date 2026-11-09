SELECT
    -1 AS status_reason_sk,
    'UNKNOWN' AS status_code,
    'Unknown Status' AS status_name,
    'UNKNOWN' AS reason_code,
    'Unknown Reason' AS reason_name,
    'Unknown' AS event_category
UNION ALL
SELECT
    status_reason_id AS status_reason_sk,
    status_code,
    status_name,
    reason_code,
    reason_name,
    event_category
FROM {{ ref('silver_status_reasons') }}