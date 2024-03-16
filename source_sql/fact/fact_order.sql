create or replace table sqlglot_demo.gold.fact_order 
as
SELECT
    ro.order_id,
    rc.customer_name,
    ro.order_status
from
    sqlglot_demo.silver.dim_order ro
left outer join sqlglot_demo.silver.dim_customer rc
on ro.customer_id = rc.customer_id
;