# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader Demonstration

# COMMAND ----------

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up locations and schema

# COMMAND ----------

ingest_query_name: str = "test_ingestion"
ingest_checkpoint_location: str = "dbfs:/FileStore/checkpoints/talks/dbx/ingestion"

# COMMAND ----------

ingest_schema = T.StructType([
  T.StructField("AssetNumber", T.StringType(), nullable=False),
  T.StructField("Ts", T.LongType(), nullable=False),
  T.StructField("Value", T.DoubleType(), nullable=False),
  T.StructField("Description", T.StringType(), nullable=True),
  T.StructField("year", T.IntegerType(), nullable=False),
  T.StructField("month", T.IntegerType(), nullable=False),
  T.StructField("day", T.IntegerType(), nullable=False)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create database and destination table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS silver

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.events
# MAGIC (
# MAGIC   AssetNumber    STRING
# MAGIC   , EventTs      BIGINT
# MAGIC   , EventDate    TIMESTAMP
# MAGIC   , DateKey      INT
# MAGIC   , Value        DOUBLE
# MAGIC   , Description  STRING
# MAGIC   , IngestYear   INT
# MAGIC   , IngestMonth  INT
# MAGIC   , IngestDay    INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/staging/demo-csv/events'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process the stream of files
# MAGIC 
# MAGIC In this section we're going to perform a simple process of
# MAGIC 
# MAGIC 1. Reading in from the input location
# MAGIC 1. Clean the data, converting the timestamp to a date and time, generating keys
# MAGIC 1. Writing out the data to the delta table

# COMMAND ----------

df: DataFrame = (
  spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.maxFilesPerTrigger", "100")
    .option("cloudFiles.validateOptions", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("header", "true")
    .schema(ingest_schema)
    .load("/mnt/landing/ingest-data")
)

# COMMAND ----------

cleaned_df: DataFrame = (
  df.withColumn("EventDate", F.col("Ts").cast("timestamp"))
    .withColumn("DateKey", F.date_format(F.col("EventDate"), "yyyyMMdd").cast("int"))
    .select(
      F.col("AssetNumber"),
      F.col("Ts").alias("EventTs"),
      F.col("EventDate"),
      F.col("DateKey"),
      F.col("Value"),
      F.col("Description"),
      F.col("year").alias("IngestYear"),
      F.col("month").alias("IngestMonth"),
      F.col("day").alias("IngestDay")
  )
)

# COMMAND ----------

(
  cleaned_df.writeStream
    .format("delta")
    .outputMode("append")
    .queryName(ingest_query_name)
    .option("checkpointLocation", ingest_checkpoint_location)
    .trigger(processingTime="15 seconds")
    .table("silver.events")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query the destination table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT COUNT(*) FROM silver.events

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT
# MAGIC   AssetNumber
# MAGIC   , CAST(EventDate AS DATE) AS Date
# MAGIC   , AVG(Value) AS AvgValue
# MAGIC FROM
# MAGIC   silver.events
# MAGIC GROUP BY
# MAGIC   AssetNumber
# MAGIC   , Date
# MAGIC ORDER BY
# MAGIC   Date
# MAGIC   , AssetNumber

# COMMAND ----------

# MAGIC %md
# MAGIC ## Close down the stream

# COMMAND ----------

for s in spark.streams.active:
  s.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ## HERE BE DRAGONS
# MAGIC This is bad, don't actually do this, this is just so that the demo can be reset

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver.events

# COMMAND ----------

dbutils.fs.rm(ingest_checkpoint_location, True)
dbutils.fs.rm("/mnt/staging/demo-csv", True)

# Note, I can't delete the ingest data because the Databricks SP only has read permissions to the raw data