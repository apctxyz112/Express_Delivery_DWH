
  
    

        create or replace transient table EXPRESS_DWH.SILVER.silver_customer_tickets
         as
        (

WITH src AS (
    SELECT
        t.ticket_id,
        t.shipment_id,
        t.customer_id,
        TRIM(t.issue_type) AS issue_type,
        TRIM(t.ticket_status) AS ticket_status,
        t.created_at,
        t.updated_at,
        t.batch_id
    FROM EXPRESS_DWH.STG.customer_tickets t
    WHERE t.batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.ticket_id ORDER BY t.updated_at DESC) = 1
)
SELECT s.*
FROM src s

        );
      
  