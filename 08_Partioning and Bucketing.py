# Databricks notebook source
# reading data from the path
employee_w_df = spark.read.csv("/FileStore/tables/employee_write_data.csv",header = True)
employee_w_df.show()

# COMMAND ----------

employee_w_df.printSchema()

# COMMAND ----------

# partitioning the file based on address column
employee_w_df.write.format("csv")\
                 .option("header", "true")\
                 .mode("overwrite")\
                 .partitionBy("address")\
                 .option("path", "/FileStore/tables/partition_by_address")\
                 .save()

# COMMAND ----------

# MAGIC %fs ls /FileStore/tables/partition_by_address

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/partition_by_address")

# COMMAND ----------


