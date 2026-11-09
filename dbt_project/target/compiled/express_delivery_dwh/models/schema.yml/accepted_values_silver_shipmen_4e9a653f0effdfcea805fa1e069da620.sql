
    
    

with all_values as (

    select
        current_status as value_field,
        count(*) as n_records

    from EXPRESS_DWH.SILVER.silver_shipments
    group by current_status

)

select *
from all_values
where value_field not in (
    'CREATED','PICKED_UP','IN_TRANSIT','DELIVERING','DELIVERY_FAILED','DELIVERED','RETURNED','CANCELLED'
)


