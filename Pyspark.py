# Databricks notebook source
# MAGIC %md
# MAGIC Load CSV to Dataframe

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
#- overwrite- overwrite the data already present in the folder
#- append - appends the data to the folder
#- ignore - ignore the operation if the folder already exists
#- errorIfExists - throw an error if the folder already exists

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

# DBTITLE 1,Cell 8
from pyspark.sql.functions import col
df1 = df1.withColumn("units_purchased", col("units_purchased") * 2)
display(df1)

# COMMAND ----------

from pyspark.sql.functions import *
d1=df1.withColumn("new_date_column", current_timestamp())
display(d1)

# COMMAND ----------

# DBTITLE 1,Cell 10
from pyspark.sql.functions import *
df1=df1.withColumn("new_word_column",lit("hello"))
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #WithColumnRename
# MAGIC
# MAGIC Used to Rename Column

# COMMAND ----------

# DBTITLE 1,Cell 12
from pyspark.sql.functions import col
df1 = df1.withColumnRenamed("new_word_column", "hello_column")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC Split - Returns a new column with array type after splitting the column by delimiter
# MAGIC
# MAGIC
# MAGIC
# MAGIC Explode
# MAGIC
# MAGIC It creates a new row for each element in the array
# MAGIC

# COMMAND ----------

# DBTITLE 1,Untitled
from pyspark.sql.functions import split, col

#Split the column Customer_name into an array
df_array = df.withColumn(
    "name_array",
    split(col("Customer_name"), "\\,")
)
display(df_array)


# COMMAND ----------

from pyspark.sql.functions import explode

df_exploded = df_array.withColumn("name_element", explode("name_array"))
display(df_exploded)

# COMMAND ----------

from pyspark.sql.functions import *

df_array_trans = df_array.withColumn("address_array", array(col("city"), col("postcode")))
display(df_array_trans)


# COMMAND ----------

# DBTITLE 1,Cell 17
df.withColumn("address_array", array_contains(col("address_array"), "NY"))
