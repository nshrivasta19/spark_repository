# Databricks notebook source
/dbfs/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz__1_.parquet

# COMMAND ----------

# reading parquet file in spark
parquet_flight_df = spark.read.parquet("/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz__1_.parquet")

# COMMAND ----------

# displaying the data
parquet_flight_df.show()

# COMMAND ----------


