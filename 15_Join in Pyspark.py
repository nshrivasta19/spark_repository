# Databricks notebook source
# MAGIC %md 
# MAGIC # Join in Pyspark

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe creation

# COMMAND ----------

# creating dataframe for customer 

cust_data = [(1,'manish','patna',2,"30-05-2022"),
(2,'vikash','kolkata',5,"12-03-2023"),
(3,'nikita','delhi',22,"25-06-2023"),
(4,'rahul','ranchi',9,"24-03-2023"),
(5,'mahesh','jaipur',23,"22-03-2023"),
(6,'prantosh','kolkata',5,"18-10-2022"),
(7,'raman','patna',7,"30-12-2022"),
(8,'prakash','ranchi',9,"24-02-2023"),
(9,'ragini','kolkata',22,"03-03-2023"),
(10,'raushan','jaipur',56,"05-02-2023")]

cust_schema = ["customer_id","customer_name","address","prd_id","date_of_joining"]

cust_df = spark.createDataFrame(data = cust_data,schema = cust_schema)

cust_df.show()

# COMMAND ----------

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

# Creating dataframe for product

product_data = [(1, 'fanta',20),
(2, 'dew',22),
(5, 'sprite',40),
(7, 'redbull',100),
(12,'mazza',45),
(22,'coke',27),
(25,'limca',21),
(27,'pepsi',14),
(56,'sting',10)]

product_schema = ["id","name","price"]

product_df = spark.createDataFrame(data = product_data,schema = product_schema)

product_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining the dataframes

# COMMAND ----------

merge_df = customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")
merge_df.show()
merge_df.explain()

# COMMAND ----------

# joining customer_df and sales_df using inner join
customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner").show()

# COMMAND ----------

# we can see that when we joined both the tables, it has merged the two tables based on the matching id as above... it also give duplicate records for all ids on how many times it got found on joining tables..for eg, customer_id 1 has ordered 3 products form product_df so there is 3 records showing after joining the table......

# COMMAND ----------

# we need to choose if we want to use distinct() or not to use with join since it is a wide transformation and expensive as join...it need to be depend on data
# we can pass the desired columns in the select statement that need to be displayed after joining two tables to avoid getting results like above one.


# COMMAND ----------

# selecting customer_id after joining both dataframes
customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")\
                  .select("customer_id").show()

# it has given us ambiguity error because we are selecting customer_id column but after joining we have 2 customer_id columns in result so it is confused which column it needs to display

# COMMAND ----------

# To resolve ambiguity issue, we need to define column_name along with dataframe name in select statement
customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner")\
                  .select(sales_df["customer_id"]).show()

# COMMAND ----------

# we can also use alias for dataframes to join two dataframes
customer_df.alias("c").join(sales_df.alias("s"),col("s.customer_id") == col("c.customer_id"),"inner").select("s.customer_id","customer_name","address","product_id").sort("product_id").show()

# COMMAND ----------

product_df.show()

# COMMAND ----------

# joining sales_df and product_df
sales_df.join(product_df,sales_df.product_id == product_df.id,"inner").show()

# we can see that there is also duplicated data for product id and if this is the case where we want to join multiple tables which are related and there will be so much duplication for example for customer_id or product_id and we need to only know how many id are required instead of how many times it is used then we can use distinct() to only get unique records

sales_df.join(product_df, sales_df["product_id"] == product_df["id"], "inner") \
    .select("product_id","id") \
    .distinct() \
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining dataframes on multiple columns

# COMMAND ----------

# joining multiple columns based on OR condition
cust_df.join(sales_df,(sales_df["customer_id"]==cust_df["customer_id"]) |
                 (sales_df["product_id"]==cust_df["prd_id"])
                 ,"inner")\
                  .show()

# it will display all the results which are satisfying first condition, second condition or both the conditions

# COMMAND ----------

# joining multiple columns based on AND condition
cust_df.join(sales_df,(sales_df["customer_id"]==cust_df["customer_id"]) &
                 (sales_df["product_id"]==cust_df["prd_id"])
                 ,"inner")\
                  .show()

# it will display only the results which are satisfying both the conditions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join
# MAGIC Returns only the rows with matching keys in both DataFrames.

# COMMAND ----------

# joining customer_df and sales_df using inner join
customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"inner").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join
# MAGIC Returns all rows from the left DataFrame and matching rows from the right DataFrame.

# COMMAND ----------

# joining customer_df and sales_df using left join
customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"left").show()

# in this type of join we can easily see which customers has never purchased any product.. we can use where condition for sales_df.customer_id.isNull()

# COMMAND ----------

result = customer_df.join(sales_df,sales_df.customer_id == customer_df.customer_id,"left")
result.filter(col("sales_df.customer_id").isNull()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join
# MAGIC Returns all rows from the right DataFrame and matching rows from the left DataFrame.

# COMMAND ----------

# joining sales_df and product_df using right join
sales_df.join(product_df,sales_df.product_id == product_df.id,"right").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Outer Join
# MAGIC Returns all rows from both DataFrames, including matching and non-matching rows.
# MAGIC Used in SCD.

# COMMAND ----------

customer_df.show()
sales_df.show()

# COMMAND ----------

# joining customer_df and sales_df using outer join
customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"outer").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Semi Join
# MAGIC Returns all rows from the left DataFrame where there is a match in the right DataFrame.

# COMMAND ----------

# joining customer_df and sales_df using left semi join
customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"left_semi").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Anti Join
# MAGIC Returns all rows from the left DataFrame where there is no match in the right DataFrame.

# COMMAND ----------

# joining customer_df and sales_df using left anti join
customer_df.join(sales_df,sales_df["customer_id"]==customer_df["customer_id"],"left_anti").show()

# in this type of join we can easily see which customers has never purchased any product.. we can use where condition for sales_df.customer_id.isNull()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross Join
# MAGIC Returns all possible combinations of rows from both the dataframes. In other words, it takes every row from one dataframe and matches it with every row in the other dataframe. The result is a new dataframe with all possible combinations of the rows from the two input dataframes.
# MAGIC
# MAGIC It is very expensive operation and should not be used.

# COMMAND ----------

sales_df.crossJoin(sales_df).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Join in Spark SQL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary view creation

# COMMAND ----------

# creting temporary view for customer, product and sales dataframes
customer_df.createOrReplaceTempView("customer_tbl")
sales_df.createOrReplaceTempView("sales_tbl")
product_df.createOrReplaceTempView("product_tbl")

# COMMAND ----------

# showing data inside customer, sales and product tables using spark sql
spark.sql("""select * from customer_tbl""").show()
spark.sql("""select * from sales_tbl""").show()
spark.sql("""select * from product_tbl""").show()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_tbl;
# MAGIC -- select * from sales_tbl;
# MAGIC -- select * from product_tbl;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner Join in SQL

# COMMAND ----------

spark.sql("""
          select * from customer_tbl
          inner join sales_tbl 
          on customer_tbl.customer_id == sales_tbl.customer_id
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Join in SQL

# COMMAND ----------

spark.sql("""
          select * from customer_tbl
          left join sales_tbl 
          on customer_tbl.customer_id == sales_tbl.customer_id
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right Join in SQL

# COMMAND ----------

spark.sql("""
          select * from product_tbl
          right join sales_tbl 
          on product_tbl.id == sales_tbl.product_id
          """).show()

# COMMAND ----------

spark.sql("""
          select * from customer_tbl
          right join sales_tbl 
          on customer_tbl.customer_id == sales_tbl.customer_id
          """).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer Join in SQL

# COMMAND ----------

spark.sql("""
          select * from customer_tbl
          full outer join sales_tbl 
          on customer_tbl.customer_id == sales_tbl.customer_id
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Semi Join in SQL

# COMMAND ----------

spark.sql("""
          select * from customer_tbl
          left semi join sales_tbl 
          on customer_tbl.customer_id == sales_tbl.customer_id
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left Anti Join in SQL

# COMMAND ----------

spark.sql("""
          select * from customer_tbl
          left anti join sales_tbl 
          on customer_tbl.customer_id == sales_tbl.customer_id
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross Join in SQL

# COMMAND ----------

spark.sql("""
          select * from customer_tbl
          cross join sales_tbl 
          """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Leetcode Problem

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataframe Creation 

# COMMAND ----------

# Creating Dataframe from Leetcode Data
person_data = [
                (1,"Wang","Allen"),
                (2,"Alice","Bob")
]
person_schema = ["personId","lastName","firstName"]

person_df = spark.createDataFrame(data = person_data, schema = person_schema)
person_df.show()

address_data = [
                  (1,2,"New York City","New York"),
                  (2,3,"Leetcode","California")
]
address_schema = ["addressId","personId","city","state"]

address_df = spark.createDataFrame(data = address_data, schema = address_schema)
address_df.show()

# COMMAND ----------

# Write a solution to report the first name, last name, city, and state of each person in the Person table. If the address of a personId is not present in the Address table, report null instead.

# COMMAND ----------

# solution in pyspark
person_df.join(address_df,address_df.personId == person_df.personId,"left")\
                .select("firstName","lastName","city","state").show()

# COMMAND ----------

# solution in spark sql
person_df.createOrReplaceTempView("person")
address_df.createOrReplaceTempView("address")

spark.sql("""
          select firstName, lastName, city, state
          from person
          left join address
          on person.personId = address.personId
          """).show()

# COMMAND ----------


