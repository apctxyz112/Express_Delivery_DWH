
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from EXPRESS_DWH.SILVER.silver_shipments

where not(weight > 0)


  
  
      
    ) dbt_internal_test