# Databricks notebook source
# we do not need to create SparkSession or SparkContext in databricks as it is already created internally.
spark

# COMMAND ----------

# spark code to read file
spark.read.csv("dbfs:/FileStore/tables/flight_data.csv",header="true").show()lo

# COMMAND ----------

# spark code to read file in detailed format
flight_df = spark.read.format("csv")\
            .option("header", "false")\
            .option("inferSchema", "false")\
            .option("mode","failfast")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

flight_df.display()

# COMMAND ----------

# spark code to read file with "header true" so that it will display column names to header correctly
flight_df_header = spark.read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "false")\
            .option("mode","failfast")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

flight_df_header.show()

# COMMAND ----------

# command to print the schema of any file
flight_df_header.printSchema()

# COMMAND ----------

## we are updating inferschema condition "true" in spark code because it is taking all the column values as string while count is integer datatype. By using inferSchema, it will read the schema of the csv and infer it
flight_df_header_schema = spark.read.format("csv")\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .option("mode","failfast")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

flight_df_header_schema.show()

# code to view schema of the file
flight_df_header_schema.printSchema()

# COMMAND ----------


