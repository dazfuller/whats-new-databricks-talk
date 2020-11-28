# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.types as T

# COMMAND ----------

schema: T.StructType = T.StructType([
  T.StructField("id", T.StringType(), nullable=True),
  T.StructField("price_paid", T.IntegerType(), nullable=True),
  T.StructField("transaction_date", T.TimestampType(), nullable=True),
  T.StructField("postcode", T.StringType(), nullable=True),
  T.StructField("property_type", T.StringType(), nullable=True),
  T.StructField("new_build", T.StringType(), nullable=True),
  T.StructField("estate_type", T.StringType(), nullable=True),
  T.StructField("paon", T.StringType(), nullable=True),
  T.StructField("saon", T.StringType(), nullable=True),
  T.StructField("street", T.StringType(), nullable=True),
  T.StructField("locality", T.StringType(), nullable=True),
  T.StructField("town_city", T.StringType(), nullable=True),
  T.StructField("district", T.StringType(), nullable=True),
  T.StructField("county", T.StringType(), nullable=True)
])

# COMMAND ----------

sample_df: DataFrame = spark.read.csv("dbfs:/FileStore/data/land-registry/pp-complete-sample.csv", schema=schema)

# COMMAND ----------

display(sample_df)