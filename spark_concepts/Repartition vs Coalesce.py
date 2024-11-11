# Databricks notebook source
# importing the functions
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %fs
# MAGIC ls FileStore/tables

# COMMAND ----------

# Reading the data of flight_data.csv by creating a dataframe
flight_df = spark.read.format("csv")\
            .option("inferschema","true")\
            .option("header","true")\
            .load("dbfs:/FileStore/tables/flight_data.csv")

# COMMAND ----------

# printing the schema of the dataframe
flight_df.printSchema()

# COMMAND ----------

# displaying the data of the dataframe
flight_df.display()

# COMMAND ----------

# displaying the total records of a dataframe using count
flight_df.count()

# COMMAND ----------

# to find no. of partitions of a dataframe, we need to convert the dataframe into rdd and then we can use getNumPartitions function to check the no. of partitions of that dataframe
flight_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ## we are repartitioning here so the data is distributed evenly for each partition... Repartitioning uses high shuffling so it is expensive

# COMMAND ----------

# converting the flight_df dataframe into 4 partitions using repartition()
partitioned_flight_df = flight_df.repartition(4)

# COMMAND ----------

# displaying the no. of partitions of the dataframe
partitioned_flight_df.rdd.getNumPartitions()

# COMMAND ----------

# creating a new column partitionId using withColumn and then displaying the count for each partition
partitioned_flight_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show()

# COMMAND ----------

# we can also repatition on a dataframe using any column
## here I am creating 300 partitions for the flight_df dataframe so partitioned_on_column will have 300 partitions from which some of the partitions will have null values
partitioned_on_column = flight_df.repartition(300)

# getting no. of partitions
partitioned_on_column.rdd.getNumPartitions()


# COMMAND ----------

# we can also repatition on a dataframe using any column
## here I am creating 300 partitions for the COLUMN "DEST_COUNTRY_NAME" so partitioned_on_column will have 300 partitions from which some of the partitions will have null values
partitioned_on_column = flight_df.repartition(300,"DEST_COUNTRY_NAME")

# getting no. of partitions
partitioned_on_column.rdd.getNumPartitions()

# COMMAND ----------

# we can also repatition on a dataframe using any column
## here I have only partitioned the dataframe based on "DEST_COUNTRY_NAME" column and have not passed any value for repartition so it will only have 1 partition
partitioned_on_column = flight_df.repartition("DEST_COUNTRY_NAME")

# getting no. of partitions
partitioned_on_column.rdd.getNumPartitions()

# COMMAND ----------

# I have created a new column for partitionId using withColumn() so that I can see the partitions 
# I have created 300 partitions for partitioned_on_column dataframe and using groupby and show to display the partitions and the records it contain.
# By looking at the result we can see that some of the partitions are null because we have only 241 records but the partitions are more than the records so remaining partitions have null values..

# it is not advisable to create a large no. of partitions since it does not have good impact on spark and we should know how to repartition data according for better optimization
partitioned_on_column.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show(300)

# COMMAND ----------

# MAGIC %md
# MAGIC ## we are using coalesce here so the data is distributed unevenly for each partition... It does not involve shuffling so it's not expensive.

# COMMAND ----------

# first we are repartitioning the dataframe for coalesce
coalesce_flight_df = flight_df.repartition(8)

# COMMAND ----------

# displaying the count of records within each partition by creating a column partitionId
coalesce_flight_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show()

# COMMAND ----------

# I am merging(coalesce) the partitions of dataframe into desired no. by using below command
three_coalesce_df = coalesce_flight_df.coalesce(3)

# COMMAND ----------

# displaying the count of records within each partition by creating a column partitionId
three_coalesce_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show()

# we can see that the record count within each partition is uneven since we have used coalesce.

# COMMAND ----------

# if we use repartition for the coalesce dataframe then the data distribution within each partition will become even.
three_repartition_df = three_coalesce_df.repartition(3)

# displaying the count of records within each partition by creating a column partitionId
three_repartition_df.withColumn("partitionId",spark_partition_id()).groupBy("partitionId").count().show()

# COMMAND ----------

# initially I have created 8 partitions of dataframe for this coalesce_flight_df in cell no. 17 and then coalesce(merge) it into 3.. which was working fine but here I have tried to coalesce into 10 so it is giving only 8 partitions bcoz decreasing in partitions can happen in coalesce but increasing cannot
coalesce_flight_df.coalesce(10).rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Repartition will have shuffling and high Input Output operation and data will be distributed evenly on each partition. we can increase or decrease no. of partitions. It is expensive due to shuffling.
# MAGIC ### Coalesce will not have any shuffling and it will merge data on provided value of partitions and data distribution will be uneven. we cannot increase no. of partitions, only decrease can perform. It is not expensive since it won't have any shuffling.
# MAGIC

# COMMAND ----------


