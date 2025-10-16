# Databricks notebook source

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Reading from source

# COMMAND ----------

df=spark.sql("select * from db_cata.silver.customers_silver") 

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplciates

# COMMAND ----------

df=df.dropDuplicates(subset=['customer_id'])
df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC diving new vs old records

# COMMAND ----------

init_load_flag =int(dbutils.widgets.get("init_load_flag"))

# COMMAND ----------

if init_load_flag ==0:
  df_old=spark.sql("select DimCustomerKey,customer_id , create_date,update_date from db_cata.gold.DimCustomers")
  
else : 
  df_old =spark.sql("select 0 DimCustomerKey,0 customer_id ,0 create_date,0 update_date from db_cata.silver.customers_silver where 1=0 ")

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming cols of df_old

# COMMAND ----------

df_old=df_old.withColumnRenamed("DimCustomerKey","old_DimCustomerKey").withColumnRenamed("customer_id","old_customer_id").withColumnRenamed("create_date","old_create_date").withColumnRenamed("update_date","old_update_date")

# COMMAND ----------

# MAGIC %md
# MAGIC Apply join with old records

# COMMAND ----------

#df_join = df.join(df_old, df.customer_id==df_old.customer_id,"left")
df_join = df.join(df_old, df["customer_id"] == df_old["old_customer_id"], "left")
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Separating new vs old records

# COMMAND ----------

df_new = df_join.filter(df_join["old_DimCustomerKey"].isNull())
df_new.limit(10).display()

# COMMAND ----------

df_old = df_join.filter(df_join["old_DimCustomerKey"].isNotNull())
df_old.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC prepparing df_old

# COMMAND ----------

#dropping all cols which are not required 
df_old=df_old.drop('old_customer_id','old_update_date')
#Renaming old_Dimcutomerkey to dimcustomerkey
df_old=df_old.withColumnRenamed("old_DimCustomerKey","DimCustomerKey")
#Renaming old_create_date to created
df_old=df_old.withColumnRenamed("old_create_date","create_date")
df_old=df_old.withColumn("create_date",to_timestamp(col("create_date")))
#recreating update date with the current timestamp
df_old=df_old.withColumn("update_date",current_timestamp())


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC preparing df_new

# COMMAND ----------

#dropping all cols which are not required 
df_new=df_new.drop('old_DimCustomerKey','old_customer_id','old_update_date','old_create_date')

#recreating update date , cureeent date with the current timestamp
df_new=df_new.withColumn("update_date",current_timestamp())
df_new=df_new.withColumn("create_date",current_timestamp())

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Surrogate key -All the values , from 1

# COMMAND ----------

df_new =df_new.withColumn("DimCustomerKey",monotonically_increasing_id()+lit(1))


# COMMAND ----------

df_new.limit(5).display()   

# COMMAND ----------

df =df.withColumn("DimCustomerKey",monotonically_increasing_id()+lit(1))


# COMMAND ----------

# MAGIC %md
# MAGIC ADDING max surrogate key

# COMMAND ----------

if init_load_flag ==1:
    max_surroage_key =0
else : 
    df_maxsur=spark.sql("select max(DimCustomerKey) as max_surrogate_key from db_cata.gold.DimCustomers")
    max_surroage_key = df_maxsur.collect()[0]['max_surrogate_key']


# COMMAND ----------

df_new =df_new.withColumn("DimCustomerKey",lit(max_surroage_key)+col("DimCustomerKey")) 

# COMMAND ----------

# MAGIC %md
# MAGIC union of df old and df new

# COMMAND ----------

df_final = df_new.unionByName(df_old)


# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC SCD type1

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

'''if init_load_flag ==1:
    df_final.write.mode("overwrite").saveAsTable("db_cata.gold.dimcustomers")
else:
    df_final.write.mode("append").saveAsTable("db_cata.gold.dimcustomers") '''

# COMMAND ----------

if (spark.catalog.tableExists("db_cata.gold.DimCustomers")):

    dlt_obj = DeltaTable.forPath(spark,"abfss://gold@dbete.dfs.core.windows.net/DimCustomers")

    dlt_obj.alias("trg").merge(df_final.alias("src"),"trg.DimCustomerKey = src.DimCustomerKey") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

else:

   df_final.write.format("delta").mode("overwrite") \
    .option("path", "abfss://gold@dbete.dfs.core.windows.net/DimCustomers") \
    .saveAsTable("db_cata.gold.DimCustomers")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_cata.gold.dimcustomers

# COMMAND ----------

