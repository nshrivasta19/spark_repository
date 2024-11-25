# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregation in Pyspark

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# creating dataframe from data

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

emp_schema = ["id","name","age","salary","country","dept"]

emp_df = spark.createDataFrame(data=emp_data,schema=emp_schema)

emp_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Count() - Acts as transformation and action 

# COMMAND ----------

# It is showing all 10 records in ...... here count is behaving like an action since it is readily evaluated
emp_df.count()

# COMMAND ----------

# here we can see that we are getting different no. of records when we are using count() for single column....because when we only count the records of a single column, count() ignores the null value from that column.
# here count is behaving like a transformation, waiting for an action like show() then it will get evaluated
emp_df.select(count("name")).show()
emp_df.select(count("id")).show()
emp_df.select(count("age")).show()
emp_df.select(count("salary")).show()
emp_df.select(count("country")).show()
emp_df.select(count("dept")).show()

# COMMAND ----------

emp_df.select(count("*")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sum()

# COMMAND ----------

emp_df.select(sum("salary")).show()
emp_df.select(sum("salary").alias("total_salary")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### min()

# COMMAND ----------

emp_df.select(min("salary").alias("minimum_salary")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### max()

# COMMAND ----------

emp_df.select(max("salary").alias("maximum_salary")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### avg()

# COMMAND ----------

emp_df.select(avg("salary").alias("average_salary")).show()

# COMMAND ----------

# using sum, min and max in a single query
emp_df.select(sum("salary").alias("total_salary"),min("salary").alias("minimum_salary"),max("salary").alias("maximum_salary")).show()

# COMMAND ----------

# using sum, count and avg in a single query
emp_df.select(sum("salary").alias("total_salary"),count("salary").alias("total_count"),avg("salary").alias("average_salary").cast("int")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### groupBy()

# COMMAND ----------

# create dataframe
data = [(1,'manish',50000,"IT"),
(2,'vikash',60000,"sales"),
(3,'raushan',70000,"marketing"),
(4,'mukesh',80000,"IT"),
(5,'pritam',90000,"sales"),
(6,'nikita',45000,"marketing"),
(7,'ragini',55000,"marketing"),
(8,'rakesh',100000,"IT"),
(9,'aditya',65000,"IT"),
(10,'rahul',50000,"marketing")]

schema = ["id","name","salary","dept"]

edf = spark.createDataFrame(data,schema)
edf.show()

# COMMAND ----------

edf.groupBy("dept").agg(sum("salary")).show()

# COMMAND ----------

data1 = [(1,"manish",50000,"IT","india"),
(2,"vikash",60000,"sales","us"),
(3,"raushan",70000,"marketing","india"),
(4,"mukesh",80000,"IT","us"),
(5,"pritam",90000,"sales","india"),
(6,"nikita",45000,"marketing","us"),
(7,"ragini",55000,"marketing","india"),
(8,"rakesh",100000,"IT","us"),
(9,"aditya",65000,"IT","india"),
(10,"rahul",50000,"marketing","us")]

schema1 = ["id","name","salary","dept","country"]

edf1 = spark.createDataFrame(data1,schema1)
edf1.show()

# COMMAND ----------

edf1.groupBy("country","dept").agg(sum("salary")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation in Spark SQL

# COMMAND ----------

# creating temporary view from dataframe
emp_df.createOrReplaceTempView("emp")

# COMMAND ----------

spark.sql("""
          select * from emp
          """).show()

# COMMAND ----------

# count
spark.sql("""
          select count(*) from emp
          """).show()

spark.sql("""
          select count(name) from emp
          """).show()          

# COMMAND ----------

# sum()
spark.sql("""
          select sum(salary) as total_salary from emp
          """).show()

# COMMAND ----------

# min()
spark.sql("""
          select min(salary) as minimum_salary from emp
          """).show()

# COMMAND ----------

# max()
spark.sql("""
          select max(salary) as maximum_salary from emp
          """).show()

# COMMAND ----------

# avg()
spark.sql("""
          select avg(salary) as average_salary from emp
          """).show()

# COMMAND ----------

# group by 
spark.sql("""
          select dept, sum(salary) 
          from emp
          group by dept 
          """).show()

# COMMAND ----------


