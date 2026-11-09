
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    shipment_number as unique_field,
    count(*) as n_records

from EXPRESS_DWH.SILVER.silver_shipments
where shipment_number is not null
group by shipment_number
having count(*) > 1



  
  
      
    ) dbt_internal_test