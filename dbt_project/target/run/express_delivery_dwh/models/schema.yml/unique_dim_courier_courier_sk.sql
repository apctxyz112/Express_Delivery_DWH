
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    courier_sk as unique_field,
    count(*) as n_records

from EXPRESS_DWH.GOLD.dim_courier
where courier_sk is not null
group by courier_sk
having count(*) > 1



  
  
      
    ) dbt_internal_test