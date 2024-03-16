create table heineken.silver.dim_customer 
as
select
    customer_id,
    customer_name
from
    heineken.bronze.raw_customers
;