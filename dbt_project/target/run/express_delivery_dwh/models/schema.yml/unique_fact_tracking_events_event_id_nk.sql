
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    event_id_nk as unique_field,
    count(*) as n_records

from EXPRESS_DWH.GOLD.fact_tracking_events
where event_id_nk is not null
group by event_id_nk
having count(*) > 1



  
  
      
    ) dbt_internal_test