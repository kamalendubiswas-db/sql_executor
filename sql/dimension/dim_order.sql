create table heineken.silver.dim_order 
as
select 
    order_id,
    customer_id,
    order_status
from
    heineken.bronze.raw_orders
;