create or replace table sqlglot_demo.silver.dim_customer 
as
select
    customer_id,
    customer_name
from
    sqlglot_demo.bronze.raw_customers
;