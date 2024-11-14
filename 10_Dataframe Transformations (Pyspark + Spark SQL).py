# Databricks notebook source
# MAGIC %md
# MAGIC # Transformation in Dataframe - Day 1
# MAGIC

# COMMAND ----------

# importing modules
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading data from file

# COMMAND ----------

# Reading data from a file using dataframe
employee_df = spark.read.format("csv")\
              .option("header","true")\
              .option("inferschema","true")\
              .option("mode","permissive")\
              .load("/FileStore/tables/employee.csv")

employee_df.show()

# Printing schema of the dataframe
employee_df.printSchema()

# COMMAND ----------

# displaying all the columns in a dataframe
employee_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Enforcement
# MAGIC

# COMMAND ----------

# defining manual schema of the dataframe
emp_schema = StructType(
  [
    StructField("id",IntegerType(),nullable =True),
    StructField("name",StringType(),nullable =True),
    StructField("age",IntegerType(),nullable =True),
    StructField("salary",IntegerType(),nullable =True),
    StructField("address",StringType(),nullable =True),
    StructField("nominee",StringType(),nullable =True),
    StructField("_corrupt_record",StringType(),nullable =True),
  ]
)

# COMMAND ----------

# Reading data from a file using predefined schema
employee_df = spark.read.format("csv")\
              .option("header","true")\
              .schema(emp_schema)\
              .option("inferschema","true")\
              .option("mode","permissive")\
              .load("/FileStore/tables/employee.csv")

employee_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ways of selecting columns

# COMMAND ----------

# selecting any column using string method
employee_df.select("name").show()
employee_df.select("name","age").show()

# COMMAND ----------

# selecting columns using column method
employee_df.select(col("name")).show()
employee_df.select(col("name"),col("age"),col("salary")).show()

# COMMAND ----------

# selecting column name with dataframe, it is useful for joins
employee_df.select(employee_df["name"],employee_df["salary"]).show()

employee_df.select(employee_df.address,employee_df.name,employee_df.age).show()

# COMMAND ----------

# selecting column using column indexing of dataframe
employee_df.select(employee_df.columns[3]).show()   # it will display the column at index 3

employee_df.select(employee_df.columns[:3]).show()  # it will display all the columns which are before index 3

# COMMAND ----------

# selecting columns using all methods in one statement
employee_df.select("id",col("name"),employee_df["age"],employee_df.address,employee_df.columns[3]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Manipulation within columns

# COMMAND ----------

# manipulating Id column using string method
employee_df.select("id + 5").show()

# It throws analysis exception bcoz it checks the value "id + 5" in the catalog if this column exists or not.. It is explained in catalyst optimation.... So it is not possible to manipulate data using string method

# COMMAND ----------

# manipulating data using column method
employee_df.select(col("id") + 5).show()

# it can be shown that the value of column id has been increemented by 5


# COMMAND ----------

# we can use expression with string method to do manipulation
employee_df.select(expr("id + 5")).show()

# COMMAND ----------

employee_df.select(expr("id as emp_id"),expr("name as emp_name"),expr("concat(name,address)")).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC # Spark SQL - Day 1

# COMMAND ----------

# creating a temporary view from dataframe to work in spark sql
employee_df.createOrReplaceTempView("emp_table")

# COMMAND ----------

# selecting all columns from table
spark.sql("""
          select * from emp_table
          """).show()

# COMMAND ----------

# selecting specified columns from table
spark.sql("""select id,age,name from emp_table""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can also see the data using sql syntax if we have hive table
# MAGIC select * from emp_table

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformations in Pyspark - Day 2

# COMMAND ----------

# # --------- Importing Modules -------------
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ----------- Creating Dataframe -------------
client_df = spark.read.format("csv")\
         .option("header","true")\
         .option("inferschema","true")\
         .option("mode","permissive")\
         .load("/FileStore/tables/client.csv")

# ----------- Displaying Dataframe ----------
client_df.display()

# ------------- Printing schema of dataframe ------------
client_df.printSchema()

# ------------- Creating Temporary view to work with spark SQL ---------
client_df.createOrReplaceTempView("client_tbl")

# COMMAND ----------

# displaying all the data from client_df dataframe
client_df.select("*").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aliasing

# COMMAND ----------

# Renaming/Aliasing the id column as emp_id using expression and displaying the result 
client_df.select(expr("id as client_id"),"name").show()

# COMMAND ----------

# Aliasing the column of the dataframe using column method and displaying other columns as string
client_df.select(col("id").alias("client_id"),"name","age","membership").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter/Where

# COMMAND ----------

# Filter and both are same... it is filtering criteria

# COMMAND ----------

client_df.filter(col("age")>=15).show()

# COMMAND ----------

client_df.where(col("age")<=25).show()

# COMMAND ----------

# Filtering data using AND condition
client_df.filter((col("membership")=="monthly") & (col("age")>20)).show()

# COMMAND ----------

# Filtering data using OR condition
client_df.filter((col("membership")=="monthly") | (col("address")=="dewas")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lietrals
# MAGIC

# COMMAND ----------

# creating a literal "shrivastava" and give column name as last_name using alias
client_df.select("*",lit("Yes").alias("active_member")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding column using withColumn()

# COMMAND ----------

client_df.withColumn("client_type",lit("Personal Training")).show()
# it will also show the newly added column temporary as it is not stored.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming column using withColumnRenamed

# COMMAND ----------

client_df.withColumnRenamed("id","client_id").show()

# COMMAND ----------

# # If we want to save intermediate results of any manipulation like we did in above examples, we can save them by assigning it in a new dataframe
client_df_updated = client_df.withColumnRenamed("id","client_id")
client_df_updated.show()

client_df_updated.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Type Casting

# COMMAND ----------

# we can see that one new column client_id is created since the first two parameters are different,i.e.,client_id and col("id")
client_df.withColumn("client_id",col("id").cast("string")).printSchema()

# COMMAND ----------

# we can see that it has overwrite the existing id column by changing its datatype
client_df.withColumn("id",col("id").cast("string"))\
      .withColumn("age",col("age").cast("float"))\
.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing Columns

# COMMAND ----------

# dropping columns from dataframe using all methods,i.e.,string, column and dataframe
client_df.drop("age",col("address"),client_df.membership).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark SQL - Day 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aliasing in SQL

# COMMAND ----------

spark.sql("""
          select id as client_id from client_tbl
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering data in SQL

# COMMAND ----------

# Filtering data using AND condition
spark.sql("""
          select * from client_tbl where age >= 22 and membership = "monthly"
          """).show()

# COMMAND ----------

# Filtering data using OR condition
spark.sql("""
          select * from client_tbl where age > 23 or membership = "monthly"
          """).show()
# it shows result for age 22 also because the second condition got fulfilled here.

# COMMAND ----------

# Filtering data using OR condition
spark.sql("""
          select * from client_tbl where age > 23 or membership = "yearly"
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Literals in spark

# COMMAND ----------

# we have added literal "shrivastava" as last name in sql... It added a new column in spark as last_name column..... It is temporary until saved
spark.sql("""
          select *, "shrivastava" as last_name from client_tbl where age >= 22 and membership = "monthly"
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adding column in SQL

# COMMAND ----------

# we have added new column in sql by concating name and last_name and aliasing as full name....... It is temporary until saved
spark.sql("""
          select *, "shrivastava" as last_name, concat(name ,last_name) as full_name
          from client_tbl where age >= 22 and membership = "monthly"
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Renaming column in sql

# COMMAND ----------

# we can alias any column with new name and it will display as a new column
spark.sql("""
          select *, id as client_id
          from client_tbl where age >= 22 and membership = "monthly"
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Type Casting in SQL

# COMMAND ----------

# changing the data type of id column and it will temporarily add a new column "id" with data type string
spark.sql("""
          select *, "shrivastava" as last_name, concat(name ,last_name) as full_name, cast(id as string)
          from client_tbl where age >= 22 and membership = "monthly"
          """).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Removing columns in SQL

# COMMAND ----------

# To remove a column in sql, just don't write the column name and it won't display in the result
spark.sql("""
          select id,name,membership
          from client_tbl 
          """).show()

# I have not added age and address column in the statement so they are discarded from the result.

# COMMAND ----------


