# Databricks notebook source
# MAGIC %md
# MAGIC # DLT pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC streaming table

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC streaming table

# COMMAND ----------

#expectations
my_rules ={
    "rule1":"product_id IS NOT NULL",
    "rule2":""
}

# COMMAND ----------

@dlt.table()
@dlt.expect_all_or_drop(my_rules)
def DimProducts_stage():
  df = spark.readStream.table("db_cata.silver.products_silver")
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC streaming view

# COMMAND ----------

@dlt.view
def DimProducts_view():
  df = spark.readStream.table("Live.DimProducts_stage")
  return df

# COMMAND ----------

# MAGIC %md
# MAGIC dim products

# COMMAND ----------

dlt.create_streaming_table("DimProducts")

# COMMAND ----------

dlt.apply_changes(
  target = "DimProducts",
  source = "Live.DimProducts_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type=2
)