SELECT
    -1 AS hub_sk,
    'UNKNOWN' AS hub_nk,
    'Unknown Hub' AS hub_name,
    'Unknown' AS hub_level,
    'Unknown' AS city,
    'Unknown' AS district,
    0 AS capacity,
    TRUE AS is_current
UNION ALL
SELECT
    hub_id AS hub_sk,
    hub_code AS hub_nk,
    hub_name,
    hub_level,
    city,
    district,
    capacity,
    TRUE AS is_current
FROM EXPRESS_DWH.SILVER.silver_hubs