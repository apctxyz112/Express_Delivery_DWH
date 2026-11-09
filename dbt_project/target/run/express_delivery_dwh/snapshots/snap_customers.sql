
      
  
    

        create or replace transient table EXPRESS_DWH.SILVER.snap_customers
         as
        (
    

    select *,
        md5(coalesce(cast(customer_id as varchar ), '')
         || '|' || coalesce(cast(updated_at as varchar ), '')
        ) as dbt_scd_id,
        updated_at as dbt_updated_at,
        updated_at as dbt_valid_from,
        
  
  coalesce(nullif(updated_at, updated_at), null)
  as dbt_valid_to
from (
        



SELECT
    customer_id,
    customer_code,
    customer_name,
    customer_type,
    membership_tier,
    phone_number,
    city,
    district,
    row_hash,
    updated_at
FROM EXPRESS_DWH.SILVER.silver_customers

    ) sbq



        );
      
  
  