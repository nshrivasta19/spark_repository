# Databricks notebook source
# MAGIC %md
# MAGIC # **Merging data in Pyspark**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Dataframe Manager_df

# COMMAND ----------

# defining data for manager 
Manager_data=[(10 ,'Anil',50000, 18),
      (11 ,'Vikas',75000, 16),
      (12 ,'Nisha',40000, 18),
      (13 ,'Nidhi',60000, 17),
      (14 ,'Priya',80000, 18),
      (15 ,'Mohit',45000, 18),
      (16 ,'Rajesh',90000, 10),
      (17 ,'Raman',55000, 16),
      (18 ,'Sam',65000, 17)]

# defining schema for manager
Manager_schema = ["id","name","salary","manager_id"]

# defining dataframe for manager
Manager_df = spark.createDataFrame(data=Manager_data, schema=Manager_schema)

# displaying manager_df data
Manager_df.show()

# printing the schema of Manager_df
Manager_df.printSchema()

# displaying the total no. of records in Manager_df
Manager_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Dataframe Manager1_df

# COMMAND ----------

# defining data for manager1
Manager1_data = [(19 ,'Sohan',50000, 18),
                 (20 ,'Sima',75000,  17)]

# defining schema for manager1
Manager1_schema = ["id","name","salary","manager_id"]

# defining dataframe for manager1
Manager1_df = spark.createDataFrame(data=Manager1_data, schema=Manager1_schema)

# displaying manager_df data
Manager1_df.show()

# printing the schema of Manager_df
Manager1_df.printSchema()

# displaying the total no. of records in Manager1_df
Manager1_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging the dataframes - union()

# COMMAND ----------

# Merging both the dataframes using union().... it will add data from both dataframes.. we have unique records in both dataframes 
Manager_df.union(Manager1_df).show()

Manager_df.union(Manager1_df).count()
# Before union there was 9 records and after union it has 11 records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging the dataframes - unionAll()

# COMMAND ----------

# Merging both the dataframes using unionAll().... it will add data from both dataframes..It works just same as union() in dataframe
Manager_df.unionAll(Manager1_df).show()

Manager_df.unionAll(Manager1_df).count()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Dataframe Manager_duplicate_data

# COMMAND ----------

# defining data for manager_duplicate 
Manager_duplicate_data=[(10 ,'Anil',50000, 18),
      (11 ,'Vikas',75000, 16),
      (12 ,'Nisha',40000, 18),
      (13 ,'Nidhi',60000, 17),
      (14 ,'Priya',80000, 18),
      (15 ,'Mohit',45000, 18),
      (16 ,'Rajesh',90000, 10),
      (17 ,'Raman',55000, 16),
      (18 ,'Sam',65000, 17),
      (18 ,'Sam',40000, 17),
      (18 ,'Sam',65000, 17)
      ]

# defining schema for manager
Manager_duplicate_schema = ["id","name","salary","manager_id"]

# defining dataframe for manager
Manager_duplicate_df = spark.createDataFrame(data=Manager_duplicate_data, schema=Manager_duplicate_schema)

# displaying manager_df data
Manager_duplicate_df.show()

# printing the schema of Manager_df
Manager_duplicate_df.printSchema()

# displaying total count of records
Manager_duplicate_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging Manager_df and Manager_duplicate_df using union()

# COMMAND ----------

# I have used union() to merge the dataframes Manager_df and Manager_duplicate_df.. Both dataframes are having same duplicate records... But it still merges all the records without removing duplicate records.

Manager_df.union(Manager_duplicate_df).show()

Manager_df.union(Manager_duplicate_df).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging Manager_df and Manager_duplicate_df using unionAll()

# COMMAND ----------

# I have used unionAll() to merge the dataframes Manager_df and Manager_duplicate_df having same records as duplicates... But it still merges all the records without removing duplicate records.

# It is clear that in Dataframe, both union and unionAll just merges the dataframes without removing the duplicate records.

Manager_df.unionAll(Manager_duplicate_df).show()

Manager_df.unionAll(Manager_duplicate_df).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating wrong_df

# COMMAND ----------

# defining new data 
wrong_column_data= [(19 ,50000, 18,'Sohan'),
                    (20 ,75000,  17,'Sima')]

# defining new schema
wrong_schema = ["id","salary","manager_id","name"]

# defining dataframe
wrong_df = spark.createDataFrame(wrong_column_data,wrong_schema)

# showing dataframe
wrong_df.show()


# COMMAND ----------

# Merging Manager1_df with wrong_df using union()... In wrong_df, the placement of column is different....But union has merged all the records from both dataframes in wrong order without checking the column names and values
Manager1_df.union(wrong_df).show()

# COMMAND ----------

# To mitigate the issue in which the columns placement is interchanged, we will use unionByName...
#  we can see it has merged the records in correct order after checking the column names and their values..
Manager1_df.unionByName(wrong_df).show()

# COMMAND ----------

# But we cannot use unionByName() for dataframe in below scenerio... 
# 1. If both the dataframe has different no. of columns -- we can manually select the column names and then use unionByName() if teh column order is interchanged
# 2. If there is difference in column names of both the dataframes -- In this case, we cannot use unionByName as it will throw error due to mismatch of column names

# COMMAND ----------

# suppose I have 2 dataframes in which 1 dataframe has less no. of columns and other dataframe has more columns or the names of columns are different in both dataframes. Creating a new dataframe for this example

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Dataframe wrong_df1

# COMMAND ----------

# defining new data 
wrong_column_data1 = [(19 ,50000, 18,'Sohan',10),
                      (20 ,75000,  17,'Sima',20)]

# defining new schema
wrong_schema1 = ["id","salary","manager_id","name","bonus"]

# defining dataframe
wrong_df1 = spark.createDataFrame(wrong_column_data1,wrong_schema1)

# showing dataframe
wrong_df1.show()


# COMMAND ----------

# merging the dataframe wrong_df and wrong_df1 using union()
wrong_df.union(wrong_df1).show()

# we can see that we have encountered with error because we have different no. of columns in both the dataframes... To avoid this situation, we need to manually select column to use union as shown in next two cell

# COMMAND ----------

wrong_df1.select("id","salary","manager_id","name").union(wrong_df).show()

# COMMAND ----------

# merging using unionByName() since the column order is interchanged in both the dataframes
wrong_df1.select("id","salary","manager_id","name").unionByName(Manager1_df).show()

# COMMAND ----------

wrong_df.union(wrong_df1.select("id","salary","manager_id","name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Dataframe wrong_df2

# COMMAND ----------

# defining new data 
wrong_column_data2 = [(19 ,50000, 18,'Sohan',10),
                      (20 ,75000,  17,'Sima',20)]

# defining new schema
wrong_schema2 = ["id","salary","manager","name","bonus"]

# defining dataframe
wrong_df2 = spark.createDataFrame(wrong_column_data2,wrong_schema2)

# showing dataframe
wrong_df2.show()

# COMMAND ----------

wrong_df1.unionByName(wrong_df2).show()
# we encountered an error while using unionByName because there is a difference in column name

# COMMAND ----------

# In above condition, we should choose union as it will merge without checking the column names
wrong_df1.union(wrong_df2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # **Merging data in Spark SQL**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating temporary views from dataframes

# COMMAND ----------

Manager_df.createOrReplaceTempView("Manager_tbl")
Manager1_df.createOrReplaceTempView("Manager1_tbl")
Manager_duplicate_df.createOrReplaceTempView("Manager_duplicate_tbl")

# COMMAND ----------

# Displaying the data of the views
spark.sql("""
          select * from Manager1_tbl
          """).show()

spark.sql("""
          select * from Manager_tbl
          """).show()

spark.sql("""
          select * from Manager_duplicate_tbl
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging the dataframes using union

# COMMAND ----------

# I have used union for merging Manager_tbl and Manager_tbl which has same records so it dropped all the duplicate columns and keeps the distinct records
spark.sql("""
          select * from Manager_tbl
          union
          select * from Manager_duplicate_tbl
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging the dataframes using union all

# COMMAND ----------

# I have used union all for merging Manager_tbl and Manager_tbl which has same records so it simply merges all the records including duplicate ones.
spark.sql("""
          select * from Manager_tbl
          union all
          select * from Manager_duplicate_tbl
          """).show()

# COMMAND ----------


