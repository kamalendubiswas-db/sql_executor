create table heineken.gold.business_order 
as
select count(1) total_order from fact_order;