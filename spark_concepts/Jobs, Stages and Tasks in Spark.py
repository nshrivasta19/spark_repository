# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# reading the data of employee_new.csv by creating a dataframe
employee_df = spark.read.format("csv")\
              .option("header","true")\
              .load("dbfs:/FileStore/tables/employee_new.csv")
print(employee_df.rdd.getNumPartitions())
employee_df = employee_df.repartition(2)
print(employee_df.rdd.getNumPartitions())
employee_df = employee_df.filter(col("salary")>50000)\
              .select("id","name","age","salary")\
              .groupBy("age").count()
employee_df.collect()


# COMMAND ----------

from pyspark.sql.functions import col

# Reading the data of employee_new.csv by creating a DataFrame
employee_df = spark.read.format("csv")\
    .option("header", "true")\
    .load("dbfs:/FileStore/tables/employee_new.csv")

# display(employee_df)
print(employee_df.rdd.getNumPartitions())

# Repartitioning the DataFrame
employee_df = employee_df.repartition(2)
print(employee_df.rdd.getNumPartitions())

# Filtering, selecting, and grouping the DataFrame
employee_df = employee_df.filter(col("salary") > 50000)\
    .select("id", "name", "age", "salary")\
    .groupBy("age").count()

# Collecting the results
employee_df.collect()

# COMMAND ----------

employee_df = spark.read.format("csv").load("dbfs:/FileStore/tables/employee_new.csv")
#employee_df.show()


# COMMAND ----------

print(employee_df.rdd.getNumPartitions())


# COMMAND ----------

employee_df = employee_df.repartition(2)

# COMMAND ----------

print(employee_df.rdd.getNumPartitions())

# COMMAND ----------


