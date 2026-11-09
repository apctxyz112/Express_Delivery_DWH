
    
    

with child as (
    select sender_customer_sk as from_field
    from EXPRESS_DWH.GOLD.fact_shipment_lifecycle
    where sender_customer_sk is not null
),

parent as (
    select customer_sk as to_field
    from EXPRESS_DWH.GOLD.dim_customer
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


