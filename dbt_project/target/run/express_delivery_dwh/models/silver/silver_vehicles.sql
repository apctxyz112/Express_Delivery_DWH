
  
    

        create or replace transient table EXPRESS_DWH.SILVER.silver_vehicles
         as
        (

WITH src AS (
    SELECT
        vehicle_id,
        TRIM(vehicle_code) AS vehicle_code,
        TRIM(vehicle_type) AS vehicle_type,
        capacity_kg,
        updated_at,
        batch_id
    FROM EXPRESS_DWH.STG.vehicles
    WHERE batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s

        );
      
  