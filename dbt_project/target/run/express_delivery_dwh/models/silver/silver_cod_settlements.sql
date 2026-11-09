
  
    

        create or replace transient table EXPRESS_DWH.SILVER.silver_cod_settlements
         as
        (

WITH src AS (
    SELECT
        c.settlement_id,
        c.shipment_id,
        c.cod_amount,
        TRIM(c.settlement_status) AS settlement_status,
        c.settled_at,
        c.updated_at,
        c.batch_id
    FROM EXPRESS_DWH.STG.cod_settlements c
    WHERE c.batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY c.settlement_id ORDER BY c.updated_at DESC) = 1
)
SELECT s.*
FROM src s

        );
      
  