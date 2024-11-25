# Databricks notebook source
# MAGIC %md
# MAGIC ### Dataframe creation

# COMMAND ----------

# importing the functions
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

emp_data = [(1,'manish',50000,'IT','m'),
(2,'vikash',60000,'sales','m'),
(3,'raushan',70000,'marketing','m'),
(4,'mukesh',80000,'IT','m'),
(5,'priti',90000,'sales','f'),
(6,'nikita',45000,'marketing','f'),
(7,'ragini',55000,'marketing','f'),
(8,'rashi',100000,'IT','f'),
(9,'aditya',65000,'IT','m'),
(10,'rahul',50000,'marketing','m'),
(11,'rakhi',50000,'IT','f'),
(12,'akhilesh',90000,'sales','m')]

emp_schema = ["id","name","salary","dept","gender"]

emp_df = spark.createDataFrame(data = emp_data, schema = emp_schema)
emp_df.show()
emp_df.printSchema()

# COMMAND ----------

emp_df.groupBy("dept").sum("salary").show()

# COMMAND ----------

# if any error encountered while aggregating like : unsupported operand type(s) for +: 'int' and 'str' 
# then it is using built in sum function so need to import sum() from pyspark.sql.functions
# from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### groupBy

# COMMAND ----------

emp_df.groupBy("dept").agg(sum("salary").alias("total_salary_by_dept")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Function

# COMMAND ----------

# using window function to sum the salary of each department.. By using groupBy we only get department and total_salary but using window function we get every record with extra column with total_salary for each department
from pyspark.sql.window import Window

window = Window.partitionBy("dept")
emp_df.withColumn("total_salary_by_dept",sum(col("salary")).over(window)).show(truncate=False)

# COMMAND ----------

# using window function to sum the salary of each department order by salary.. 
from pyspark.sql.window import Window

window = Window.partitionBy("dept").orderBy("salary")
emp_df.withColumn("total_salary_by_dept",sum(col("salary")).over(window)).show(truncate=False)

# COMMAND ----------

# this code shows the row number for every row within a window,i.e., windows created for this code is IT, marketing and sales
window = Window.partitionBy("dept").orderBy("salary")
emp_df.withColumn("row",row_number().over(window)).show(truncate=False)

# COMMAND ----------

# this code shows the row number, rand and dense rank 
from pyspark.sql.window import Window

window = Window.partitionBy("dept").orderBy("salary")
emp_df.withColumn("row_number",row_number().over(window))\
        .withColumn("rank",rank().over(window))\
           .withColumn("dense_rank",dense_rank().over(window))\
.show(truncate=False)

# COMMAND ----------

# this code shows the row number, rand and dense rank.. In this code, we will partition by dept and gender column so windows will be increased and it will rank according to gender and dept wise
from pyspark.sql.window import Window

window = Window.partitionBy("dept","gender").orderBy("salary")
emp_df.withColumn("row_number",row_number().over(window))\
        .withColumn("rank",rank().over(window))\
           .withColumn("dense_rank",dense_rank().over(window))\
.show(truncate=False)

# COMMAND ----------

# Calculating top 2 earner from each department

window = Window.partitionBy("dept").orderBy(desc("salary"))
emp_df.withColumn("row_number",row_number().over(window))\
        .withColumn("rank",rank().over(window))\
            .withColumn("dense_rank",dense_rank().over(window))\
                .filter(col("dense_rank") <=2)\
                .show(truncate=False)


# COMMAND ----------

# Calculating top 2 earner from each department with gender wise

window = Window.partitionBy("dept","gender").orderBy(desc("salary"))
emp_df.withColumn("row_number",row_number().over(window))\
        .withColumn("rank",rank().over(window))\
            .withColumn("dense_rank",dense_rank().over(window))\
                .filter(col("dense_rank") <=2)\
                .show(truncate=False)

# COMMAND ----------


