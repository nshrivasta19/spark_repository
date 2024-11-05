# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %fs
# MAGIC ls FileStore/tables

# COMMAND ----------

flight_df = spark.read.format("csv")\
            .option("inferschema","true")\
            .option("header","true")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

# COMMAND ----------

flight_df.printSchema()

# COMMAND ----------

flight_df.display()

# COMMAND ----------

flight_df.count()

# COMMAND ----------

# to find no. of partitions of a dataframe, we need to convert the dataframe into rdd and then we can use getNumPartitions function to check the no. of partitions of that dataframe
flight_df.rdd.getNumPartitions()

# COMMAND ----------


