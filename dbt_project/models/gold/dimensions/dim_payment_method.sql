SELECT
    -1 AS payment_method_sk,
    'UNKNOWN' AS payment_method_nk,
    'Unknown Payment Method' AS payment_method_name,
    'Unknown' AS payer_type,
    FALSE AS has_cod
UNION ALL
SELECT
    payment_method_id AS payment_method_sk,
    payment_method_code AS payment_method_nk,
    payment_method_name,
    payer_type,
    has_cod
FROM {{ ref('silver_payment_methods') }}