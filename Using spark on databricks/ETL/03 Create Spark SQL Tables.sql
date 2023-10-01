-- Databricks notebook source
create database if not exists banana_bronze

-- COMMAND ----------

use banana_bronze

-- COMMAND ----------

create external table if not exists ${table_name}
using parquet
options (
  path='${bronze_base_dir}/${table_name}'
)