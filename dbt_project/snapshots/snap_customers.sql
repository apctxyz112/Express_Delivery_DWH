{% snapshot snap_customers %}

{{
    config(
        target_schema='SILVER',
        unique_key='customer_id',
        strategy='check',
        check_cols=['row_hash'],
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    customer_code,
    customer_name,
    customer_type,
    membership_tier,
    phone_number,
    city,
    district,
    row_hash,
    updated_at
FROM {{ ref('silver_customers') }}

{% endsnapshot %}