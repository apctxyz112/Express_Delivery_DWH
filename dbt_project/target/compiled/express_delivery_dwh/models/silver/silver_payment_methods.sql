

WITH src AS (
    SELECT
        payment_method_id,
        TRIM(payment_method_code) AS payment_method_code,
        TRIM(payment_method_name) AS payment_method_name,
        TRIM(payer_type) AS payer_type,
        has_cod,
        updated_at,
        batch_id
    FROM EXPRESS_DWH.STG.payment_methods
    WHERE batch_id = 'AF_manual__2026-11-09T11:01:20.574764+00:00'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY payment_method_id ORDER BY updated_at DESC) = 1
)
SELECT s.*
FROM src s
