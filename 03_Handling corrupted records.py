# Databricks notebook source
# MAGIC %md
# MAGIC # Handling corrupted records in spark

# COMMAND ----------

# MAGIC %md
# MAGIC we have a csv file named employee_new.csv which has some corrupted records. we will see how these corrupted records are getting handled in different read modes. Also we will look how to store these corrupted records.

# COMMAND ----------

# reading the file in permissive read mode. It will display all the records including corrupted one and set null if any field is missing and ignores the extra value in the row 3 and 4 it ignored nominee3 and nominee4
employee_df = spark.read.format("csv")\
              .option("header", "true")\
              .option("inferSchema", "true")\
              .option("mode","permissive")\
              .load("dbfs:/FileStore/tables/employee.csv")

employee_df.display()

# COMMAND ----------

# reading the file in dropMalformed read mode. It will drop all the corrupted records
employee_df = spark.read.format("csv")\
              .option("header", "true")\
              .option("inferSchema", "true")\
              .option("mode","dropMalformed")\
              .load("dbfs:/FileStore/tables/employee.csv")

employee_df.show()

# COMMAND ----------

# reading the file in failfast read mode. It throws an exception when it meets corrupted records.
employee_df = spark.read.format("csv")\
              .option("header", "true")\
              .option("inferSchema", "true")\
              .option("mode","failFast")\
              .load("dbfs:/FileStore/tables/employee.csv")

employee_df.display()

# COMMAND ----------

employee_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating a column to print corrupted records using schema enforcement/creation
# MAGIC

# COMMAND ----------

# importing the datatypes for schema creation
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# defining the schema
emp_schema = StructType(
    [
        StructField("id", IntegerType(),True),
        StructField("name", StringType(),True),
        StructField("age", IntegerType(),True),
        StructField("salary", IntegerType(),True),
        StructField("address", StringType(), nullable = True),
        StructField("nominee", StringType(), nullable = True),
        StructField("_corrupt_record", StringType(), True)
    ]
)

# COMMAND ----------

# reading the data by defining manually created schema with permissive mode to store corrupt records
employee_df = spark.read.format("csv")\
              .option("header", "true")\
              .option("inferSchema", "true")\
              .schema(emp_schema)\
              .option("mode","permissive")\
              .load("dbfs:/FileStore/tables/employee.csv")

#employee_df.display()
employee_df.show()

# COMMAND ----------

# we can see that the value of _corrupt_record is truncated and not displaying full result. To view the full record from that column we will  set truncate property to false in show()

employee_df = spark.read.format("csv")\
              .option("header", "true")\
              .option("inferSchema", "true")\
              .schema(emp_schema)\
              .option("mode","permissive")\
              .load("dbfs:/FileStore/tables/employee.csv")

#employee_df.display()
employee_df.show(truncate=False)

# COMMAND ----------

##### Storing the corrupted records on a path using badrecordspath option

employee_df = spark.read.format("csv")\
              .option("header", "true")\
              .option("inferSchema", "true")\
              .schema(emp_schema)\
              .option("badrecordspath","/FileStore/tables/bad_records")\
              .load("dbfs:/FileStore/tables/employee.csv")

#employee_df.display()
employee_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC below command is used to view files on a location

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/tables/'

# COMMAND ----------

# MAGIC %fs ls 'dbfs:/FileStore/tables/bad_records/20240925T025810/bad_records'

# COMMAND ----------

# creating a dataframe to view stored bad records 
bad_records_df = spark.read.format("json").load("dbfs:/FileStore/tables/bad_records/20240917T065221/bad_records/")
## bad_records_df.display()
bad_records_df.show(truncate=False)

# COMMAND ----------


