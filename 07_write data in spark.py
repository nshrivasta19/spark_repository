# Databricks notebook source
# reading data from the path
employee_write_df = spark.read.csv("/FileStore/tables/employee_write_data.csv",header = True)

# COMMAND ----------

employee_write_df.show()

# COMMAND ----------

# writing data to a path using overwrite mode.... if file already present, it will overwrite the file
employee_write_df.write.format("csv")\
                 .option("header","true")\
                 .mode("overwrite")\
                 .option("path","/FileStore/tables/csv_write")\
                 .save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/csv_write")

# COMMAND ----------

# overwriting the file in 3 partition
employee_write_df.repartition(3).write.format("csv")\
                 .option("header", "true")\
                 .mode("overwrite")\
                 .option("path", "/FileStore/tables/csv_write")\
                 .save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/csv_write")

# COMMAND ----------

# writing data in append mode.. it just added the new file along with the existing file
employee_write_df.write.format("csv")\
                 .option("header", "true")\
                 .mode("append")\
                 .option("path", "/FileStore/tables/csv_write")\
                 .save()

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/csv_write")

# COMMAND ----------

# writing data in errorIfExists mode.. It throws an error bcoz the file already exists in the path
employee_write_df.write.format("csv")\
                 .option("header", "true")\
                 .mode("errorIfExists")\
                 .option("path", "/FileStore/tables/csv_write")\
                 .save()

# COMMAND ----------

# writing data in ignore mode.. It ignores to perform any action if the file already exists in the path
employee_write_df.write.format("csv")\
                 .option("header", "true")\
                 .mode("ignore")\
                 .option("path", "/FileStore/tables/csv_write")\
                 .save()

# COMMAND ----------

# I have used ignore mode in this cell also but since there is no file in the path, it generates one csv file.
employee_write_df.write.format("csv")\
                 .option("header", "true")\
                 .mode("ignore")\
                 .option("path", "/FileStore/tables/csv_write_new")\
                 .save()

# COMMAND ----------

# MAGIC %fs ls "/FileStore/tables/csv_write_new"

# COMMAND ----------

# Reading the data we have write on above cells.
employee_read = spark.read.csv("/FileStore/tables/csv_write_new", header = True)
employee_read.display()

# COMMAND ----------


