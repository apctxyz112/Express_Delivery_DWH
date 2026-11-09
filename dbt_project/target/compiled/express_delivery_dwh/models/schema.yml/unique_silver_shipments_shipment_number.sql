
    
    

select
    shipment_number as unique_field,
    count(*) as n_records

from EXPRESS_DWH.SILVER.silver_shipments
where shipment_number is not null
group by shipment_number
having count(*) > 1


