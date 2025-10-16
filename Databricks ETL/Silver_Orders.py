# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading

# COMMAND ----------

df=spark.read.format("parquet").load(f"abfss://bronze@dbete.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

df =df.withColumnRenamed("_rescued_data", "rescued_data")
display(df)

# COMMAND ----------

df = df.drop("rescued_data")
display(df)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

df = df.withColumn("order_date", to_timestamp(col('order_date')))
display(df)

# COMMAND ----------

from pyspark.sql.functions import year

df = df.withColumn("year", year(col("order_date")))
display(df)

# COMMAND ----------

#Ranking total amount , top to down , so it's window function 
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
df1 = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
display(df1)

# COMMAND ----------

df1 = df1.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
display(df1)

# COMMAND ----------

df1 = df1.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create classes- OOP

# COMMAND ----------

class windows:

  def dense_rank(self,df):
    df_dense_rank= df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
    return df_dense_rank
  
  def rank(self,df):
    df_rank= df.withColumn("rank_flag", rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
    return df_rank
  
  def row_number(self,df):
    df_row_number= df.withColumn("row_flag", row_number().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
    return df_row_number

# COMMAND ----------

df_new =df
df_new.display()

# COMMAND ----------

obj=windows()
df_result = obj.dense_rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data writing

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@dbete.dfs.core.windows.net/orders")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE db_cata.silver.orders_silver
# MAGIC USING delta
# MAGIC LOCATION 'abfss://silver@dbete.dfs.core.windows.net/orders';

# COMMAND ----------



# COMMAND ----------

