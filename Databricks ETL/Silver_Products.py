# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df=spark.read.format("parquet").load(f"abfss://bronze@dbete.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

df=df.drop("_rescued_data")
df.display()


# COMMAND ----------

#temporary view
df.createOrReplaceTempView("products")


# COMMAND ----------

# MAGIC %md
# MAGIC # Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION db_cata.bronze.discount_func(p_price DOUBLE)
# MAGIC RETURNS DOUBLE
# MAGIC LANGUAGE SQL
# MAGIC RETURN p_price *0.90

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,db_cata.bronze.discount_func(price) as discounted_price
# MAGIC from products

# COMMAND ----------

df=df.withColumn("discounted_price",expr("db_cata.bronze.discount_func(price)"))
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC python way now 

# COMMAND ----------

# MAGIC %sql
# MAGIC create OR replace function db_cata.bronze.upper_func(p_brand string)
# MAGIC returns string
# MAGIC language PYTHON
# MAGIC as
# MAGIC $$
# MAGIC   return p_brand.upper()
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,brand,db_cata.bronze.upper_func(brand) as brand_upper
# MAGIC from products
# MAGIC

# COMMAND ----------

df.write.format("delta").mode("overwrite").option("path","abfss://silver@dbete.dfs.core.windows.net/products").save()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE db_cata.silver.products_silver
# MAGIC USING delta
# MAGIC LOCATION 'abfss://silver@dbete.dfs.core.windows.net/products';

# COMMAND ----------

