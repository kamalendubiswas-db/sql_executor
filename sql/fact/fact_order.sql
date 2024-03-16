create table heineken.gold.fact_order 
as
SELECT
    ro.order_id,
    rc.customer_name,
    ro.order_status
from
    heineken.silver.dim_order ro
left outer join heineken.silver.dim_customer rc
on ro.customer_id = rc.customer_id
;