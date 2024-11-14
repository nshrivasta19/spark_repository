# Databricks notebook source
# MAGIC %md
# MAGIC # Creating dataframes from data 
# MAGIC

# COMMAND ----------

spark

# COMMAND ----------

# creating data from tuple
new_data = [
(1,   1),   
(2,   1),   
(3,   1),  
(4,   2),   
(6,   2),   
(7,   2),  
]

# COMMAND ----------

# creating schema in list
new_schema = ["id","num"]

# COMMAND ----------

# creating dataframe from data and schema
new_df = spark.createDataFrame(new_data,new_schema)

# COMMAND ----------

new_df.show()

# COMMAND ----------


