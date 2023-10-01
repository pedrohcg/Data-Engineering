# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC drop database if exists banana_bronze cascade

# COMMAND ----------

dbutils.fs.rm(dbutils.widgets.get('bronze_base_dir'), recurse=True)

# COMMAND ----------

dbutils.fs.rm(dbutils.widgets.get('gold_base_dir'), recurse=True)