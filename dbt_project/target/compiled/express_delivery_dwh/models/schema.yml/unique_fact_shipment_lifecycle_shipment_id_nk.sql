
    
    

select
    shipment_id_nk as unique_field,
    count(*) as n_records

from EXPRESS_DWH.GOLD.fact_shipment_lifecycle
where shipment_id_nk is not null
group by shipment_id_nk
having count(*) > 1


