# Databricks notebook source
df=spark.read.table("db_cata.bronze.regions")
df.display()

# COMMAND ----------

df=df.drop("_rescued_data")

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@dbete.dfs.core.windows.net/regions")

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@dbete.dfs.core.windows.net/regions")
df.display()

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@dbete.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@dbete.dfs.core.windows.net/customers")
df.display()

# COMMAND ----------

df=spark.read.format("delta").load("abfss://silver@dbete.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE db_cata.silver.regions_silver
# MAGIC USING delta
# MAGIC LOCATION 'abfss://silver@dbete.dfs.core.windows.net/regions';