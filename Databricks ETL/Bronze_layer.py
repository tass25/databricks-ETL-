# Databricks notebook source
# MAGIC %md
# MAGIC # Dynamic capabilities

# COMMAND ----------

dbutils.widgets.text("file_name","")

# COMMAND ----------

p_filename=dbutils.widgets.get("file_name")

# COMMAND ----------

p_filename

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df=spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet").option("cloudFiles.schemaLocation",f"abfss://bronze@dbete.dfs.core.windows.net/checkpoint_{p_filename}").load(f"abfss://source@dbete.dfs.core.windows.net/{p_filename}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Writing

# COMMAND ----------

df.writeStream.format("parquet").outputMode("append").option("checkpointLocation",f"abfss://bronze@dbete.dfs.core.windows.net/checkpoint_{p_filename}").option("path",f"abfss://bronze@dbete.dfs.core.windows.net/{p_filename}").trigger(once=True).start()

# COMMAND ----------

#dbutils.fs.rm(f"abfss://bronze@dbete.dfs.core.windows.net/{p_filename}/_spark_metadata", recurse=True)


# COMMAND ----------

dt = spark.read.format("parquet").load(f"abfss://bronze@dbete.dfs.core.windows.net/{p_filename}")
dt.display()

# COMMAND ----------

df=spark.read.format("parquet").load(f"abfss://bronze@dbete.dfs.core.windows.net/{p_filename}")
df.display()