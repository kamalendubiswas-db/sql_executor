create or replace table sqlglot_demo.gold.business_order 
as
with order_count as (
select count(1) as total_order from sqlglot_demo.gold.fact_order)
select * from order_count;