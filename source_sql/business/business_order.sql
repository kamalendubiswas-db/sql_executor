create or replace table sqlglot_demo.gold.business_order 
as
select count(1) total_order from sqlglot_demo.gold.fact_order;