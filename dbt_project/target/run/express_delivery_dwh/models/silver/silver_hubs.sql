
  
    

        create or replace transient table EXPRESS_DWH.SILVER.silver_hubs
         as
        (

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
    FROM EXPRESS_DWH.STG.hubs
    WHERE batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY hub_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s

        );
      
  