# Databricks notebook source
# MAGIC %md
# MAGIC Load CSV to Dataframe

# COMMAND ----------

ggg

# COMMAND ----------

# DBTITLE 1,Cell 2
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/databricks_simulated_retail_customer_data/v01/source_files")
#df.show()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Load dataframe to CSV

# COMMAND ----------

df.write.format("csv").mode("overwrite").save("/Volumes/test_catalog/schema__t1/volume__t1")
#this will give multiple files in the folder because:
#Spark Splits the DataFrame into partitions
#Each partition is written by one executor
#Each partition → one file

# COMMAND ----------

# DBTITLE 1,Cell 5
df.coalesce(1).write.format("csv").mode("overwrite").save("/Volumes/test_catalog/schema__t1/volume__t1")
#This will give me one file in the folder because:
#.coalesce(1) → 1 partition
#but not efficient therefore should not be used. 

#Other saving modes are:
#- overwrite
#- append
#- ignore
#- errorIfExists

# COMMAND ----------

# MAGIC %md
# MAGIC WithColumn
# MAGIC Used for:
# MAGIC - adding new column
# MAGIC - updating a value
# MAGIC - change datatype
# MAGIC
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC df = df.withColumn("column_name", expression)

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark.sql.functions import col 
df1=df.withColumn("customer_id",col("customer_id").cast("int"))
display(df1)


# COMMAND ----------

# MAGIC %md
# MAGIC
