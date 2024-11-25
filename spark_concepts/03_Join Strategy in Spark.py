# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# setting no. of partitions
# spark.conf.set("spark.sql.shuffle.partitions",5)

# creating dataframe for customer 

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema = ["customer_id","customer_name","address","date_of_joining"]

customer_df = spark.createDataFrame(data = customer_data,schema = customer_schema)

customer_df.show()

# Creating dataframe for sales

sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema = ["customer_id","product_id","quantity","date_of_purchase"]

sales_df = spark.createDataFrame(data = sales_data,schema = sales_schema)

sales_df.show()

# joining the dataframes
merge_df = customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")
merge_df.show()

# explaining it's working/physical plan
merge_df.explain()


# COMMAND ----------

merge_df = customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")
merge_df.show()
merge_df.explain()

# it will filter out the null values as null values can't be joined
# sort merge join is the default join strategy used by spark
# when we select the column of join condition by it's name then we get ambiguity error but when we select using * it gets displayed because inside it is identified by id instead of name


# COMMAND ----------

# setting no. of partitions
spark.conf.set("spark.sql.shuffle.partitions",5)

# creating dataframe for customer 

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema = ["customer_id","customer_name","address","date_of_joining"]

customer_df = spark.createDataFrame(data = customer_data,schema = customer_schema)

customer_df.show()

# Creating dataframe for sales

sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema = ["customer_id","product_id","quantity","date_of_purchase"]

sales_df = spark.createDataFrame(data = sales_data,schema = sales_schema)

sales_df.show()

# joining the dataframes
merge_df = customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")
merge_df.show()

# explaining it's working/physical plan
merge_df.explain()


# COMMAND ----------

spark

# COMMAND ----------

# setting no. of partitions
spark.conf.set("spark.sql.shuffle.partitions",5)

# creating dataframe for customer 

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema = ["customer_id","customer_name","address","date_of_joining"]

customer_df = spark.createDataFrame(data = customer_data,schema = customer_schema)

customer_df.show()

# Creating dataframe for sales

sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema = ["customer_id","product_id","quantity","date_of_purchase"]

sales_df = spark.createDataFrame(data = sales_data,schema = sales_schema)

sales_df.show()

# joining the dataframes
merge_df = customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")
# merge_df.show()

# explaining it's working/physical plan
merge_df.explain()

# joining the dataframes using broadcast hash join strategy
broadcast_df = customer_df.join(broadcast(sales_df),customer_df["customer_id"]==sales_df["customer_id"],"inner")
broadcast_df.show()
broadcast_df.explain()


# COMMAND ----------

# command to get the default value of broadcast file size 
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
# command to set the default value of broadcast file size 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",20971520)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------


