
      
  
    

        create or replace transient table EXPRESS_DWH.SILVER.snap_couriers
         as
        (
    

    select *,
        md5(coalesce(cast(courier_id as varchar ), '')
         || '|' || coalesce(cast(updated_at as varchar ), '')
        ) as dbt_scd_id,
        updated_at as dbt_updated_at,
        updated_at as dbt_valid_from,
        
  
  coalesce(nullif(updated_at, updated_at), null)
  as dbt_valid_to
from (
        



SELECT
    courier_id,
    courier_code,
    courier_name,
    hub_id,
    contract_type,
    vehicle_id,
    effective_date,
    row_hash,
    updated_at
FROM EXPRESS_DWH.SILVER.silver_couriers

    ) sbq



        );
      
  
  