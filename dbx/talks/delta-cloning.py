# Databricks notebook source
# MAGIC %md
# MAGIC # Delta cloning
# MAGIC 
# MAGIC Cloning allows us to create a copy of an existing delta table into a new table. There are 2 methods for doing this, _Shallow_ and _Deep_, in both cases we can work with the clone without affecting the original table.

# COMMAND ----------

import re

# COMMAND ----------

# MAGIC %md
# MAGIC ## Some setup

# COMMAND ----------

current_user: str = re.sub(r"[^\w\d\_]", "_", spark.sql("SELECT current_user()").head()[0])
current_user_db: str = re.sub(r"\_+", "_", current_user)

# Create a database for the current user
spark.sql(f"CREATE DATABASE IF NOT EXISTS `{current_user_db}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a shallow clone
# MAGIC 
# MAGIC This will create a clone of the database by referencing the source data, but any changes are made in the cloned copy. If someone runs `vacuum` on the source though then this copy will break

# COMMAND ----------

shallow_clone_sql: str = f"""
CREATE TABLE IF NOT EXISTS `{current_user_db}`.events
SHALLOW CLONE silver.events
LOCATION 'dbfs:/FileStore/Users/{current_user_db}/silver_test'
"""
  
spark.sql(shallow_clone_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the contents of the clone

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/FileStore/Users/{current_user_db}/silver_test")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lets make some changes

# COMMAND ----------

display(spark.sql(f"SELECT * FROM `{current_user_db}`.events"))

# COMMAND ----------

# Lets delete an asset
asset_id: str = spark.sql(f"SELECT AssetNumber FROM `{current_user_db}`.events LIMIT 1").head()[0]
initial_count: int = spark.sql(f"SELECT COUNT(*) FROM `{current_user_db}`.events").head()[0]

spark.sql(f"DELETE FROM `{current_user_db}`.events WHERE AssetNumber = '{asset_id}'")
updated_count: int = spark.sql(f"SELECT COUNT(*) FROM `{current_user_db}`.events").head()[0]
  
print(f"Removed {initial_count - updated_count} records")

# COMMAND ----------

# Now lets check the contents of the clone
dbutils.fs.ls(f"dbfs:/FileStore/Users/{current_user_db}/silver_test")

# COMMAND ----------

# Validate that the cloned data has the asset data removed
display(spark.sql(f"SELECT * FROM `{current_user_db}`.events WHERE AssetNumber = '{asset_id}'"))

# COMMAND ----------

# And validate that the original data is still intact
display(spark.sql(f"SELECT * FROM silver.events WHERE AssetNumber = '{asset_id}'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS `{current_user_db}`.events")
spark.sql(f"DROP DATABASE IF EXISTS `{current_user_db}`")

dbutils.fs.rm(f"dbfs:/FileStore/Users/{current_user_db}", True)