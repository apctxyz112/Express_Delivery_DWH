
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select courier_sk
from EXPRESS_DWH.GOLD.dim_courier
where courier_sk is null



  
  
      
    ) dbt_internal_test