-- Databricks notebook source
use banana_bronze

-- COMMAND ----------

show tables

-- COMMAND ----------

insert overwrite directory '${gold_base_dir}/daily_product_revenue'
using parquet
select o.order_date, oi.order_item_product_id, round(sum(oi.order_item_subtotal), 2) revenue
from orders o
join order_items oi
on o.order_id = oi.order_item_order_id
where o.order_status in ('COMPLETE', 'CLOSED')
group by 1, 2

-- COMMAND ----------

select * from parquet.`${gold_base_dir}/daily_product_revenue`
order by 1, 2 desc