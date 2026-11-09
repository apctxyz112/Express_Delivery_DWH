
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select origin_hub_sk as from_field
    from EXPRESS_DWH.GOLD.fact_shipment_lifecycle
    where origin_hub_sk is not null
),

parent as (
    select hub_sk as to_field
    from EXPRESS_DWH.GOLD.dim_hub
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test