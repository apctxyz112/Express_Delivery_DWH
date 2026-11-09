SELECT
    -1 AS vehicle_sk,
    'UNKNOWN' AS vehicle_nk,
    'Unknown Vehicle' AS vehicle_type,
    0::NUMBER(10, 2) AS capacity_kg
UNION ALL
SELECT
    vehicle_id AS vehicle_sk,
    vehicle_code AS vehicle_nk,
    vehicle_type,
    capacity_kg
FROM EXPRESS_DWH.SILVER.silver_vehicles