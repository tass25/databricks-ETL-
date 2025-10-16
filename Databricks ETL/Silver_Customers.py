# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

df=spark.read.format("parquet").load(f"abfss://bronze@dbete.dfs.core.windows.net/customers")
df.display()

# COMMAND ----------

df=df.drop("_rescued_data")
df.display()


# COMMAND ----------

df=df.withColumn("domains",split(col("email"),"@")[1])
df.display()


# COMMAND ----------

df.groupBy("domains").agg(count("customer_id").alias("total_customers")).sort("total_customers",ascending=False).display()


# COMMAND ----------

df_gmail=df.filter(col("domains")=="gmail.com")
df_gmail.display()

# COMMAND ----------

df=df.withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name")))
df=df.drop("first_name","last_name")
df.display()


# COMMAND ----------

df.write.mode("overwrite").format("delta").save("abfss://silver@dbete.dfs.core.windows.net/customers")

# COMMAND ----------

#%sql
#create schema db_cata.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS db_cata.silver.customers_silver;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE db_cata.silver.customers_silver
# MAGIC USING delta
# MAGIC LOCATION 'abfss://silver@dbete.dfs.core.windows.net/customers';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_cata.silver.customers_silver

# COMMAND ----------

