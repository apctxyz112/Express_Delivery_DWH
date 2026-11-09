
  
    

        create or replace transient table EXPRESS_DWH.SILVER.silver_status_reasons
         as
        (

WITH src AS (
    SELECT
        status_reason_id,
        TRIM(status_code) AS status_code,
        TRIM(status_name) AS status_name,
        TRIM(reason_code) AS reason_code,
        TRIM(reason_name) AS reason_name,
        TRIM(event_category) AS event_category,
        updated_at,
        batch_id
    FROM EXPRESS_DWH.STG.status_reasons
    WHERE batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status_reason_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s

        );
      
  