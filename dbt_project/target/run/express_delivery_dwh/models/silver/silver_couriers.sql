
  
    

        create or replace transient table EXPRESS_DWH.SILVER.silver_couriers
         as
        (

WITH src AS (
    SELECT
        courier_id,
        TRIM(courier_code) AS courier_code,
        TRIM(courier_name) AS courier_name,
        hub_id,
        TRIM(contract_type) AS contract_type,
        vehicle_id,
        effective_date,
        MD5(
            COALESCE(TRIM(courier_name), '') || '|' ||
            COALESCE(TO_VARCHAR(hub_id), '-1') || '|' ||
            COALESCE(TRIM(contract_type), '') || '|' ||
            COALESCE(TO_VARCHAR(vehicle_id), '-1')
        ) AS row_hash,
        updated_at,
        batch_id
    FROM EXPRESS_DWH.STG.couriers
    WHERE batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY courier_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s

        );
      
  