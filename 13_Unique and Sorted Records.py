# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# creating dataframe from list data

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)
     ]

schema = ["id","name","salary","manager_id"]

manager_df = spark.createDataFrame(data=data,schema=schema)
manager_df.show()
manager_df.count()

# this dataframe has duplicate records..

# COMMAND ----------

# MAGIC %md
# MAGIC ### Selecting Unique Records

# COMMAND ----------

manager_df.distinct().show()
manager_df.distinct().count()
# Initially we had 13 records and after using distinct function we have left with 10 records... so exact replica of records has been discarded in it

# COMMAND ----------

manager_df.distinct("id","name").show()

# COMMAND ----------

# if we want to select unique rows from certain columns then we should select those columns before applying distinct().... we should not pass anything inside distinct function as shown in above cell
manager_df.select("id",manager_df.name).distinct().show()
manager_df.select("id",manager_df.name).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping Duplicate Records

# COMMAND ----------

# Passing every column name inside the function since it can't take * for all columns
# saving the results in new dataframe
dropped_manager_df = manager_df.dropDuplicates(["id","name","salary","manager_id"])

# It is visible that id 14 has appeared twice in the result because it is not exact replica of the record.. there is a difference in salary for both record

# COMMAND ----------

dropped_manager_df.show()

# COMMAND ----------

manager_df.dropDuplicates().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sorting the data

# COMMAND ----------

# sorting the data based on salary
dropped_manager_df.sort("salary").show()

# COMMAND ----------

# sorting the data based on id
dropped_manager_df.sort(col("id")).show()

# COMMAND ----------

# sorting the data in descending order... By default, sorting is in ascending....If you want to sort data in descending while using string method then we need to use expression.
dropped_manager_df.sort(col("salary").desc()).show()

# COMMAND ----------

# sorting the data on descending order on two columns
manager_df.sort(col("salary").desc(),col("name").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Leetcode Problem
# MAGIC Find the names of the customer that are not referred by the customer with id = 2.

# COMMAND ----------

# creating dataframe from leetcode data
leet_code_data = [
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)
]

schema = ["id","name","referee_id"]

leet_df = spark.createDataFrame(leet_code_data,schema)
leet_df.show()

# COMMAND ----------

leet_df.select("name").where((col("referee_id")!= 2) | (col("referee_id").isNull())).show()

# COMMAND ----------


