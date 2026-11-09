{% snapshot snap_couriers %}

{{
    config(
        target_schema='SILVER',
        unique_key='courier_id',
        strategy='check',
        check_cols=['row_hash'],
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    courier_id,
    courier_code,
    courier_name,
    hub_id,
    contract_type,
    vehicle_id,
    effective_date,
    row_hash,
    updated_at
FROM {{ ref('silver_couriers') }}

{% endsnapshot %}