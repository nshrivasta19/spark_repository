# Databricks notebook source
# MAGIC %md
# MAGIC # Partitioning in Spark

# COMMAND ----------

# Reading the data from the path using dataframe... We will write this data into another path
employee_w_df = spark.read.csv("/FileStore/tables/employee_write_data.csv",header = True)
employee_w_df.show()

# Printing the schema of the dataframe
employee_w_df.printSchema()

# COMMAND ----------

# Partitioning the file based on address column
employee_w_df.write.format("csv")\
                 .option("header", "true")\
                 .mode("overwrite")\
                 .partitionBy("address")\
                 .option("path", "/FileStore/tables/partition_by_address")\
                 .save()

# COMMAND ----------

# MAGIC %md
# MAGIC There are two ways to show the directories inside a folder.
# MAGIC 1. Using %fs ls "File path"
# MAGIC 2. Using dbutils command
# MAGIC Both are shown below.

# COMMAND ----------

# MAGIC %fs ls "/FileStore/tables/partition_by_address"

# COMMAND ----------

# Listing the directories inside the path
dbutils.fs.ls("/FileStore/tables/partition_by_address")

# COMMAND ----------

# Partitioning the file with address and gender column
employee_w_df.write.format("csv")\
                 .option("header", "true")\
                 .mode("overwrite")\
                 .partitionBy("address","gender")\
                 .option("path", "/FileStore/tables/partition_by_address_gender")\
                 .save()

# COMMAND ----------

# Displaying the folders inside the path
dbutils.fs.ls("/FileStore/tables/partition_by_address_gender")

dbutils.fs.ls("/FileStore/tables/partition_by_address_gender/address=INDIA")

# We can see that gender folder has been created inside address folder for each country

# COMMAND ----------

# We need to be aware of the order of columns that we want to partition. For eg, if i will change the order for partitions columns address and gender then the result will completely change
employee_w_df.write.format("csv")\
                 .option("header", "true")\
                 .mode("overwrite")\
                 .partitionBy("gender","address")\
                 .option("path", "/FileStore/tables/partition_by_gender_address")\
                 .save()

# COMMAND ----------

# displaying the folders inside the path
dbutils.fs.ls("/FileStore/tables/partition_by_gender_address")

# we can see that instead of address, gender folders has been created for both female and male... Inside these gender folders there will be folders for country which has records for how many girls or boys are from which country

# COMMAND ----------

# displaying the folders inside the path
dbutils.fs.ls("/FileStore/tables/partition_by_gender_address/gender=f/")

# COMMAND ----------

# If we don't have any column that has repeated value like address, in that case if we use partitioning then it will create partition for every single record which is not a good thing for optimization purpose. 
# example -
employee_w_df.write.format("csv")\
             .option("header","true")\
             .mode("overwrite")\
             .option("path","/FileStore/tables/partition_by_id")\
             .partitionBy("id")\
             .save()

# COMMAND ----------

# displaying the result for above use case
dbutils.fs.ls("/FileStore/tables/partition_by_id")

# we can see that it has created 15 partitions for all 15 records.

# COMMAND ----------

# To avoid creating partition for each single record if our file has random data then in that particular case we will use bucketing.

# COMMAND ----------

# MAGIC %md
# MAGIC # Bucketing in Spark

# COMMAND ----------

employee_w_df.write.format("csv")\
             .option("header","true")\
             .mode("overwrite")\
             .option("path","/FileStore/tables/bucket_by_id")\
             .bucketBy(3,"id")\
             .saveAsTable("bucket_by_id_table")

# In hive table, whole data is stored but in the path location data has been divided and stored in 3 chunks/buckets of data

# COMMAND ----------

dbutils.fs.ls("/FileStore/tables/bucket_by_id")

# COMMAND ----------

# Reading the file from the location
spark.read.csv("dbfs:/FileStore/tables/bucket_by_id/part-00000-tid-414496629680521024-3d5edf77-761d-40ed-ac9d-2faa282cc9de-77-3_00002.c000.csv",header=True).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If I want to read the data from the hive table instead of file system then I need to use SQL command
# MAGIC select * from bucket_by_id_table

# COMMAND ----------

# In bucketing, there are possibility of creation of too many buckets for the data. For eg, if my data has 200 tasks and I have choose to create 3 buckets to store my data. In that case, instead of creating 3 buckets Spark will create 600 buckets.. 3 buckets for each task....

# To avoid creation of too many buckets, we can first repartition the data and then use bucketing
employee_w_df.repartition(3).write.format("csv")\
             .option("header","true")\
             .mode("overwrite")\
             .option("path","/FileStore/tables/bucket_by_id")\
             .bucketBy(2,"id")\
             .saveAsTable("bucket_by_id_table")

# COMMAND ----------

# displaying the files inside the folder
dbutils.fs.ls("/FileStore/tables/bucket_by_id")

# COMMAND ----------

employee_w_df.repartition(5,"id").write.format("csv")\
             .option("header","true")\
             .mode("overwrite")\
             .option("path","/FileStore/tables/bucket_by_id")\
             .bucketBy(2,"id")\
             .saveAsTable("bucket_by_id_table")

# COMMAND ----------

# displaying the files inside the folder
dbutils.fs.ls("/FileStore/tables/bucket_by_id")

# COMMAND ----------


