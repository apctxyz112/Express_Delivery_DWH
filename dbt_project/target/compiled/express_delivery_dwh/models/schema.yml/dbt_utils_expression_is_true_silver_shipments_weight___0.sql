



select
    1
from EXPRESS_DWH.SILVER.silver_shipments

where not(weight > 0)

