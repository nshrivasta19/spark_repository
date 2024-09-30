# Databricks notebook source
# MAGIC %md
# MAGIC # Creating schema manually / Schema Enforcement

# COMMAND ----------

# we will run the spark dataframe code with schema enforcement by defining manually created schema here.
flight_df = spark.read.format("csv")\
            .option("header", "false")\
            .option("inferSchema", "false")\
            .schema(my_schema)\
            .option("mode","failfast")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

flight_df.display()
# this code got failed because we have not defined my_schema in spark yet.
# we should not use any schema in code before defining it in the spark. Always define it first then use it in the code otherwise you will face the issue that schema used before schema defined in production.

# COMMAND ----------

# importing the libraries
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

# defining the schema by manually creation
my_schema = StructType(
    [
        StructField("DEST_COUNTRY_NAME", StringType(), True),
        StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
        StructField("count", IntegerType(), False),
    ]
)

# COMMAND ----------

# using my_schema in spark dataframe code
flight_df = spark.read.format("csv")\
            .option("header", "false")\
            .option("inferSchema", "false")\
            .schema(my_schema)\
            .option("mode","failfast")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

flight_df.display()
# this code failed because we have corrupted record in the data. I will change the mode to permissive to view the corrupted record in the next cell.

# COMMAND ----------

# using my_schema in spark dataframe code with mode = permissive
flight_df = spark.read.format("csv")\
            .option("header", "false")\
            .option("inferSchema", "false")\
            .schema(my_schema)\
            .option("mode","permissive")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

flight_df.display()

# in this result we saw that the first row is corrupted. Since we have set the header = false, it reads the header from the imported file as a row which gives the error. 

# COMMAND ----------

# If we have defined the schema manually for the table but the file already has a header then we can skip that row using skiprow option.
flight_df = spark.read.format("csv")\
            .option("header", "false")\
            .option("skiprows",1)\
            .option("inferSchema", "false")\
            .schema(my_schema)\
            .option("mode","permissive")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

flight_df.show()

# COMMAND ----------


