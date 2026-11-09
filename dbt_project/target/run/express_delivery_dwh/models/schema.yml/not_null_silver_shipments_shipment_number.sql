
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select shipment_number
from EXPRESS_DWH.SILVER.silver_shipments
where shipment_number is null



  
  
      
    ) dbt_internal_test