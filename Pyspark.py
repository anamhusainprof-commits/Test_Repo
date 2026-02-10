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
df_map = spark.createDataFrame(data, schema)

# Display
df_map.show(truncate=False)
df_map.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC .getItem()-->It is used to get the value of a key from a map

# COMMAND ----------

from pyspark.sql.functions import col

# Access the 'math' score from the 'scores' map using getItem
df_with_math = df_map.withColumn("math_score", col("scores").getItem("math"))
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
df_with_math_grade = df_map.withColumn(
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
df_alias = df_map.select(col("student_id").alias("id_alias"))
display(df_alias)

# asc: Sort DataFrame by a column in ascending order
df_asc = df_map.orderBy(col("student_id").asc())
display(df_asc)

# desc: Sort DataFrame by a column in descending order
df_desc = df_map.orderBy(col("student_id").desc())
display(df_desc)

# like: Filter rows where a column matches a pattern
df_like = df.filter(col("ship_to_address").like("IN%"))
display(df_like)


# COMMAND ----------

# MAGIC %md
# MAGIC Filter() or Where()
# MAGIC
# MAGIC Both are used to filter values from a column

# COMMAND ----------

# DBTITLE 1,Cell 28
from pyspark.sql.functions import *
df_state = df.filter(col('state').like('IN'))
display(df_state)

# COMMAND ----------

from pyspark.sql.functions import *
df_state=df.where(col('state').like('IN'))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC distinct() = “remove exact same rows”
# MAGIC
# MAGIC dropDuplicates() = “remove duplicates based on chosen columns”

# COMMAND ----------

# MAGIC %md
# MAGIC distinct():
# MAGIC Used to remove duplicate rows from a DataFrame.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 32
from pyspark.sql.functions import *

# Case 1: Get distinct values from a single column
df_distinct_state = df.select('state').distinct()
display(df_distinct_state)

# Case 2: Get distinct combinations of multiple columns
df_distinct_state_city = df.select('state', 'city').distinct()
display(df_distinct_state_city)

# Case 3: Get distinct rows across all columns
df_distinct_all = df.distinct()
display(df_distinct_all)

# COMMAND ----------

# Drop duplicate rows based on all columns
df_no_duplicates = df.dropDuplicates()
display(df_no_duplicates)

# Drop duplicate rows based on specific columns, e.g., 'state' and 'city'
df_no_duplicates_specific = df.dropDuplicates(['state', 'city'])
display(df_no_duplicates_specific)

# COMMAND ----------

# MAGIC %md
# MAGIC orderBy(): Sorts the DataFrame rows based on one or more columns, ascending by default.
# MAGIC You can specify ascending or descending order for each column.
# MAGIC
# MAGIC sort(): Equivalent to orderBy(), sorts the DataFrame rows based on specified columns.
# MAGIC
# MAGIC Example usage:
# MAGIC df.orderBy("column_name")
# MAGIC df.sort("column_name")

# COMMAND ----------

# 'sort' and 'orderBy' are used to sort DataFrames by one or more columns.
# Both methods are equivalent in Spark.

# Example: Sort by 'student_id' in ascending order
df_sorted_asc = df_map.sort("student_id")
display(df_sorted_asc)

# Example: Sort by 'student_id' in descending order
df_sorted_desc = df_map.sort(col("student_id").desc())
display(df_sorted_desc)

# 'orderBy' works the same way
df_ordered_asc = df_map.orderBy("student_id")
display(df_ordered_asc)

df_ordered_desc = df_map.orderBy(col("student_id").desc())
display(df_ordered_desc)

# COMMAND ----------

# DBTITLE 1,Cell 37
# Ensure both DataFrames have the same columns in the same order
common_cols = [col for col in df.columns if col in df1.columns]
df1_common = df1.select(*common_cols)
df_common = df.select(*common_cols)

# union: Returns the union of two DataFrames, removing duplicate rows
df_union = df1_common.union(df_common)
display(df_union)

# unionAll: Deprecated and not supported in PySpark 3.x and above. Use union instead.
# If you need to keep duplicates, union already does that.
# There is no need to use unionAll.

#To combine two PySpark DataFrames and ensure there are no duplicate rows, use union() followed by distinct()


# COMMAND ----------

# MAGIC %md
# MAGIC #Aggregate() and GroupBy()

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC Understanding Aggregate and Group By in PySpark & SQL
# MAGIC
# MAGIC What is Group By?
# MAGIC *Group By* is used to split data into groups based on the values of one or more columns. Each group contains rows that share the same value(s) in the specified column(s).
# MAGIC
# MAGIC **Example:**
# MAGIC - Grouping sales data by `region` to analyze each region separately.
# MAGIC
# MAGIC What is Aggregate?
# MAGIC *Aggregate* refers to applying summary functions (like SUM, AVG, COUNT, MIN, MAX) to each group created by `groupBy`. This produces a single result per group, summarizing the data.
# MAGIC
# MAGIC **Example:**
# MAGIC - Calculating the total sales (`SUM`) or average sales (`AVG`) for each region.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 38
from pyspark.sql.functions import avg, sum, count, expr

# groupBy: Groups the DataFrame using the specified column(s) and applies aggregate functions.

# Example: Group by 'state' and calculate the average 'units_purchased' (use try_cast to tolerate malformed input)
df_grouped_avg = df.groupBy("state").agg(expr("avg(try_cast(units_purchased as double))").alias("avg_units_purchased"))
display(df_grouped_avg)

# Example: Group by 'state' and 'city', and calculate the sum of 'units_purchased' (use try_cast)
df_grouped_sum = df.groupBy("state", "city").agg(expr("sum(try_cast(units_purchased as double))").alias("total_units"))
display(df_grouped_sum)

# Example: Group by 'state' and count the number of rows in each group
df_grouped_count = df.groupBy("state").agg(count("*").alias("row_count"))
display(df_grouped_count)

# GroupBy without aggregate: Returns a GroupedData object, not a DataFrame.
grouped_data = df.groupBy("state")

# To see the effect, you need to apply an aggregate or use .count(), .agg(), etc.
# If you want to get unique groups as a DataFrame, use .select('state').distinct()
df_groups = df.select("state").distinct()
display(df_groups)

# COMMAND ----------

# GroupBy without aggregate: Returns a GroupedData object, not a DataFrame.
grouped_data = df.groupBy("state")

# To see the effect, you need to apply an aggregate or use .count(), .agg(), etc.
# If you want to get unique groups as a DataFrame, use .select('state').distinct()
df_groups = df.select("state").distinct()
display(df_groups)

# COMMAND ----------

#unionByName combines two DataFrames by matching columns based on their names, not their positions.
#If columns are missing in one DataFrame, you can set allowMissingColumns=True to fill those columns with nulls.
#This is useful when DataFrames have different column orders or some columns are missing.
#The resulting DataFrame contains all columns from both DataFrames, with rows aligned by column name.

# COMMAND ----------

from pyspark.sql import Row

# Create two DataFrames with different columns and order
data_a = [Row(id="A1", name="Alice", age=30), Row(id="A2", name="Bob", age=25)]
data_b = [Row(name="Charlie", id="B1", city="NY"), Row(name="David", id="B2", city="LA")]

df_a = spark.createDataFrame(data_a)
df_b = spark.createDataFrame(data_b)

# unionByName: Combines DataFrames by matching column names, not positions
df_union_by_name = df_a.unionByName(df_b, allowMissingColumns=True)
display(df_union_by_name)

# COMMAND ----------

from pyspark.sql import Row

# Create sample DataFrames
data_left = [Row(id=1, name="Alice"), Row(id=2, name="Bob"), Row(id=3, name="Charlie")]
data_right = [Row(id=2, city="NY"), Row(id=3, city="LA"), Row(id=4, city="SF")]

df_left = spark.createDataFrame(data_left)
df_right = spark.createDataFrame(data_right)

display(df_left)
display(df_right)

# Inner Join: Returns rows with matching keys in both DataFrames
df_inner = df_left.join(df_right, on="id", how="inner")
display(df_inner)

# Left Outer Join: Returns all rows from the left DataFrame and matched rows from the right DataFrame
df_left_outer = df_left.join(df_right, on="id", how="left")
display(df_left_outer)

# Right Outer Join: Returns all rows from the right DataFrame and matched rows from the left DataFrame
df_right_outer = df_left.join(df_right, on="id", how="right")
display(df_right_outer)

# Full Outer Join: Returns all rows when there is a match in either left or right DataFrame
df_full_outer = df_left.join(df_right, on="id", how="outer")
display(df_full_outer)

# Left Semi Join: Returns rows from the left DataFrame where there is a match in the right DataFrame (no columns from right)
df_left_semi = df_left.join(df_right, on="id", how="left_semi")
display(df_left_semi)

# Left Anti Join: Returns rows from the left DataFrame where there is no match in the right DataFrame
df_left_anti = df_left.join(df_right, on="id", how="left_anti")
display(df_left_anti)

# COMMAND ----------

from pyspark.sql.functions import col, lower# Advanced join examples with additional conditions and column selection


# Inner Join with additional condition (e.g., name starts with 'A')
df_inner_adv = df_left.join(df_right, on="id", how="inner").where(col("name").startswith("A"))
display(df_inner_adv)

# Left Outer Join selecting specific columns and renaming
df_left_outer_adv = df_left.join(df_right, on="id", how="left") \
    .select(df_left["id"], df_left["name"], df_right["city"].alias("joined_city"))
display(df_left_outer_adv)

# Right Outer Join with filter (e.g., city is not null)
df_right_outer_adv = df_left.join(df_right, on="id", how="right").filter(col("city").isNotNull())
display(df_right_outer_adv)

# Full Outer Join with coalesce to fill missing values
from pyspark.sql.functions import coalesce, lit
df_full_outer_adv = df_left.join(df_right, on="id", how="outer") \
    .select(
        coalesce(df_left["id"], df_right["id"]).alias("id"),
        coalesce(df_left["name"], lit("Unknown")).alias("name"),
        coalesce(df_right["city"], lit("Unknown")).alias("city")
    )
display(df_full_outer_adv)

# Left Semi Join with additional filter (e.g., id > 1)
df_left_semi_adv = df_left.join(df_right, on="id", how="left_semi").filter(col("id") > 1)
display(df_left_semi_adv)

# Left Anti Join with additional filter (e.g., name contains 'a')
df_left_anti_adv = df_left.join(df_right, on="id", how="left_anti").filter(lower(col("name")).contains("a"))
display(df_left_anti_adv)


# COMMAND ----------

# MAGIC %md
# MAGIC json **multiline**

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #fillna
# MAGIC

# COMMAND ----------

# fillna is a DataFrame method used to replace null (None/NaN) values with specified values.
# You can provide a single value to fill all nulls, or a dictionary to fill nulls in specific columns.
# This is useful for data cleaning and ensuring downstream operations do not fail due to missing values.

# Example: Fill all nulls with 0
df_filled = df.fillna(0)

# Example: Fill nulls in specific columns#
#If state is missing, put "Unknown"
#If units_purchased is missing, put 0”
df_filled_cols = df.fillna({"state": "Unknown", "units_purchased": 0})

display(df_filled)
display(df_filled_cols)

# COMMAND ----------

# sample() is a DataFrame method that returns a random sample of rows.
# It takes a fraction (between 0 and 1) to specify the proportion of the dataset to sample.
# You can set 'withReplacement=True' to allow repeated rows, and 'seed' for reproducibility.

# Example: Sample 20% of the DataFrame without replacement
df_sampled = df.sample(withReplacement=False, fraction=0.2, seed=42)
display(df_sampled)

# Example: Sample 50% of the DataFrame with replacement
df_sampled_wr = df.sample(withReplacement=True, fraction=0.5, seed=123)
display(df_sampled_wr)

# COMMAND ----------

# collect() retrieves all rows of a DataFrame as a list of Row objects on the driver.
# Use with caution for large DataFrames, as it can cause memory issues.

rows = df.collect()  # Returns a list of Row objects
# Example: Access the first row
first_row = rows[0]

# COMMAND ----------

# createOrReplaceTempView registers a DataFrame as a temporary SQL table (view) in Spark.
# If a view with the same name exists, it is replaced.
# This allows you to run SQL queries on the DataFrame using spark.sql().
# The view exists only for the duration of the Spark session.

# Example: Register df_map as a temporary view named 'student_scores'
df_map.createOrReplaceTempView("student_scores")

# Now you can query it using SQL
#SELECT student_id FROM student_scores WHERE scores>= 90
#result = spark.sql("SELECT student_id, scores['math'] AS math_score FROM student_scores WHERE scores['math'] >= 90")
#display(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lower(student_id) FROM student_scores

# COMMAND ----------

# MAGIC %skip
# MAGIC # createOrReplaceGlobalTempView registers a DataFrame as a global temporary view in Spark.
# MAGIC # Global temp views are accessible across all Spark sessions using the 'global_temp' database.
# MAGIC # The view persists until the Spark application terminates.
# MAGIC
# MAGIC #GLOBAL TEMPORARY VIEW is not supported on serverless compute. SQLSTATE: 0A000
# MAGIC
# MAGIC # Example: Register df_map as a global temp view named 'student_scores_global'
# MAGIC df_map.createOrReplaceGlobalTempView("student_scores_global")
# MAGIC
# MAGIC # Query the global temp view using the 'global_temp' database prefix
# MAGIC #result = spark.sql("SELECT student_id, scores['math'] AS math_score FROM global_temp.student_scores_global WHERE scores['math'] >= 90")
# MAGIC #display(result)

# COMMAND ----------

# A UDF (User Defined Function) allows you to define custom functions in Python and use them in Spark DataFrame operations.
# UDFs are useful when built-in Spark functions do not provide the required functionality.
# UDFs can be registered and used in DataFrame transformations or SQL queries.

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define a Python function
def greet(name):
    return f"Hello, {name}!"

# Register the function as a UDF
greet_udf = udf(greet, StringType())

# Apply the UDF to a DataFrame column
df_with_greeting = df_left.withColumn("greeting", greet_udf(df_left["name"]))
display(df_with_greeting)

# COMMAND ----------

# MAGIC %md
# MAGIC
