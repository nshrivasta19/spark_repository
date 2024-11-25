# Databricks notebook source
# MAGIC %md
# MAGIC ## Pyspark - conditional statement

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# Creating dataframe 
emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,"",None,None,None),
(7,'adam',37,65000,'us','IT')
]

emp_schema = ["id","name","age","salary","country","department"]

emp_df = spark.createDataFrame(emp_data,emp_schema)

emp_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### when otherwise

# COMMAND ----------

# using when otherwise statement(if else) to create a column and update its adultness based on age factor
emp_df.withColumn("adult",when(col("age")>18,"Yes")
                          .when(col("age")<18,"No")
                          .otherwise("No Value")).show()

# COMMAND ----------

# we have some of the values as null so to fix that issue will fix the null columns by giving a literal to it.
emp_df.withColumn("age",when(col("age").isNull(),lit(19))
                  .otherwise(col("age")))\
        .withColumn("adult",when(col("age")>18,"yes")
                    .otherwise("No")).show()

# COMMAND ----------

# arranging the people according to multiple age conditions in different age categories
emp_df2 = emp_df.withColumn("age_category",when((col("age")>0) & (col("age")<18),"Minor")
                                  .when((col("age")>18) & (col("age")<40),"Mid")
                                    .otherwise("Major"))\
                                      .show()
                                      

# COMMAND ----------

emp_df2.display()

# COMMAND ----------

# example no. 2
data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)]
columns = ["name","gender","salary"]
df = spark.createDataFrame(data = data, schema = columns)
df.show()

# COMMAND ----------

# In this example, based on check condition, assigned the male and female and also handled null value as well.. along with storing the result in new dataframe
df2 = df.withColumn("new_gender",when(col("gender")=="M","male")
                                  .when(col("gender")=="F","female")
                                  .when(col("gender").isNull(),"Unknown")
                                  .otherwise(col("gender"))   
                    )
df2.show()

# COMMAND ----------

# Another way to use when otherwise in pyspark
df3 = df.withColumn("new_gender",when(df.gender =="M","male")
                                  .when(df.gender =="F","female")
                                  .when(df.gender.isNull(),"Unknown")
                                  .otherwise(df.gender)      
                    )
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark SQL - conditional statement

# COMMAND ----------

emp_df.createOrReplaceTempView("emp_table")
emp_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### case when

# COMMAND ----------

# applying case when condition in sql to filter out people on age category based on age range
spark.sql("""
          select *, case when age>18 then "Major"
                            when age<18 then "Minor"
                              else "No Value"
                                end as adult         
                              from emp_table
          """).show()

# COMMAND ----------


