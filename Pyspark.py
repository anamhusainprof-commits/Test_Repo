# Databricks notebook source
# MAGIC %md
# MAGIC Load CSV to Dataframe--Read

# COMMAND ----------

# DBTITLE 1,Cell 2
df=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/databricks_simulated_retail_customer_data/v01/source_files")
#df.show()
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Load dataframe to CSV-Write

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
#Update data type
from pyspark.sql.functions import col 
df1=df.withColumn("customer_id",col("customer_id").cast("int"))
display(df1)


# COMMAND ----------

# DBTITLE 1,Cell 8
#Update Value
from pyspark.sql.functions import col
df1 = df1.withColumn("units_purchased", col("units_purchased") * 2)
display(df1)

# COMMAND ----------

#new column addition
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
# MAGIC Split() - Returns a new column with array type after splitting the column by delimiter
# MAGIC
# MAGIC
# MAGIC
# MAGIC Explode() - It creates a new row for each element in the given array.
# MAGIC
# MAGIC Array()- It creates a new array column by merging data from various columns
# MAGIC
# MAGIC ArrayContains()- It is used to check if array column contains a value. Returns True/False value

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
#explodes the name array to different rows for each element
df_exploded = df_array.withColumn("name_element", explode("name_array"))
display(df_exploded)

# COMMAND ----------

from pyspark.sql.functions import *
#creates a new column with an array after combining city and postcode
df_array_trans = df_array.withColumn("address_array", array(col("city"), col("postcode")))
display(df_array_trans)


# COMMAND ----------

# DBTITLE 1,Cell 17
from pyspark.sql.functions import array_contains, col

# Create a new column 'address_has_NY' that is True if 'address_array' contains 'NY'
df_with_flag = df_array_trans.withColumn("address_has_NY", array_contains(col("address_array"), "NY"))

display(df_with_flag)  # Databricks
# or
#df_with_flag.show()    # Standard PySpark


# COMMAND ----------

# MAGIC %md
# MAGIC MapType()
# MAGIC It is used to represent map key value pairs simislar to python dictionary.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType
from pyspark.sql import Row

# Sample data (15 rows)
data = [
    Row(id="S01", scores={"math": 85, "science": 78, "english": 88}),
    Row(id="S02", scores={"math": 92, "science": 81, "english": 90}),
    Row(id="S03", scores={"math": 76, "science": 74, "english": 80}),
    Row(id="S04", scores={"math": 89, "science": 90, "english": 87}),
    Row(id="S05", scores={"math": 91, "science": 85, "english": 93}),
    Row(id="S06", scores={"math": 68, "science": 72, "english": 75}),
    Row(id="S07", scores={"math": 84, "science": 79, "english": 82}),
    Row(id="S08", scores={"math": 95, "science": 94, "english": 96}),
    Row(id="S09", scores={"math": 73, "science": 70, "english": 77}),
    Row(id="S10", scores={"math": 88, "science": 86, "english": 84}),
    Row(id="S11", scores={"math": 90, "science": 88, "english": 91}),
    Row(id="S12", scores={"math": 82, "science": 80, "english": 83}),
    Row(id="S13", scores={"math": 79, "science": 76, "english": 78}),
    Row(id="S14", scores={"math": 87, "science": 85, "english": 89}),
    Row(id="S15", scores={"math": 93, "science": 92, "english": 94})
]

# Schema definition
schema = StructType([
    StructField("student_id", StringType()),
    StructField("scores", MapType(StringType(), IntegerType()))
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Display
df.show(truncate=False)
df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC .getItem()-->It is used to get the value of a key from a map

# COMMAND ----------

from pyspark.sql.functions import col

# Access the 'math' score from the 'scores' map using getItem
df_with_math = df.withColumn("math_score", col("scores").getItem("math"))
display(df_with_math)

# COMMAND ----------

# MAGIC %md
# MAGIC Row Class & Column Class

# COMMAND ----------

# MAGIC %md
# MAGIC When() & Otherwise()
# MAGIC It is similar to If-If Else

# COMMAND ----------

from pyspark.sql.functions import when, col

# Use 'when' to create a new column 'math_grade' based on 'math' score
df_with_math_grade = df.withColumn(
    "math_grade",
    when(col("scores").getItem("math") >= 90, "A")
    .when(col("scores").getItem("math") >= 80, "B")
    .otherwise("C")
)

display(df_with_math_grade)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 26
display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Cell 26
from pyspark.sql.functions import col, expr

# alias: Rename a column for use in expressions or select
df_alias = df.select(col("student_id").alias("id_alias"))
display(df_alias)

# asc: Sort DataFrame by a column in ascending order
df_asc = df.orderBy(col("student_id").asc())
display(df_asc)

# desc: Sort DataFrame by a column in descending order
df_desc = df.orderBy(col("student_id").desc())
display(df_desc)

# like: Filter rows where a column matches a pattern
df_like = df.filter(col("student_id").like("S0%"))
display(df_like)

