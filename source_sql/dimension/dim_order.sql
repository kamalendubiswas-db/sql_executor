create or replace table sqlglot_demo.silver.dim_order 
as
select 
    order_id,
    customer_id,
    order_status
from
    sqlglot_demo.bronze.raw_orders
;