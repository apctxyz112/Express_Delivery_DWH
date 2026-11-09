
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select shipment_id_nk
from EXPRESS_DWH.GOLD.fact_shipment_lifecycle
where shipment_id_nk is null



  
  
      
    ) dbt_internal_test