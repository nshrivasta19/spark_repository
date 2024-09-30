# Databricks notebook source
# reading the json file with single line delimited records
spark.read.format("json")\
.option("InferSchema","True")\
.option("mode","permissive")\
.load("/FileStore/tables/line_delimited_records.json")\
.display()

# COMMAND ----------

# reading the json file with single line delimited records but since the json file contains comma after each record it is taking one extra row will all null values. To avoid the row, we will use drop command

line_delimited_df = spark.read.format("json")\
.option("InferSchema","True")\
.load("dbfs:/FileStore/tables/line_delimited_json.json")\

# filtering out rows where all columns are null
non_null_df = line_delimited_df.na.drop("all")

non_null_df.display()

# COMMAND ----------

# reading the single line json delimited records with extra key
spark.read.format("json")\
.option("InferSchema","True")\
.option("mode","permissive")\
.load("dbfs:/FileStore/tables/single_file_json_with_extra_fields.json")\
.display()

# COMMAND ----------

# reading the multi line json records
spark.read.format("json")\
.option("InferSchema","True")\
.option("mode","permissive")\
.load("dbfs:/FileStore/tables/Multi_line_correct.json")\
.show()

# this code throws analysis exception since we have not enabled multiline option here, which is by default fault.

# COMMAND ----------

# reading the multi line json records by setting multiline property to true to avoid analysis exception
spark.read.format("json")\
.option("InferSchema","True")\
.option("MULTILINE","TRUE")\
.option("mode","permissive")\
.load("dbfs:/FileStore/tables/Multi_line_correct.json")\
.show()

# COMMAND ----------

# reading the multi line json incorrect records
spark.read.format("json")\
.option("InferSchema","True")\
.option("MULTILINE","TRUE")\
.option("mode","permissive")\
.load("dbfs:/FileStore/tables/Multi_line_incorrect.json")\
.show()

# this code only returns 1 row because in the json file the records are presented as a set of dictionary so after reading the first set, it considered there are no further records. We should always pass multiline records as list.
# [{set1},{set2},{set3}]

# COMMAND ----------

# reading the corrupted json records
spark.read.format("json")\
.option("InferSchema","True")\
.option("mode","permissive")\
.load("dbfs:/FileStore/tables/corrupted_json.json")\
.show(truncate=False)

# this code automatically displays the corrupted record in _corrupt_record file after encounter a corrupted record.

# COMMAND ----------

# reading the nested json records
spark.read.format("json")\
.option("InferSchema","True")\
.option("mode","permissive")\
.load("dbfs:/FileStore/tables/zomato_file5.json")\
.display()


# COMMAND ----------


