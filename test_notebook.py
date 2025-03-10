# Databricks notebook source
# MAGIC %md
# MAGIC ## Introduction to this notebook
# MAGIC This notebook we created on the fly in the workshop. I did stumble on a few things, and I want to add some explainations what went wrong and provide the corrected result:
# MAGIC * "select * from values({'a': 1, 'b': 2})"
# MAGIC The correct syntax for spark is of course:
# MAGIC "select * from values(map('a',1,'b',2))", and the reason for the confusion is probably a bit too much programming in other languages where dictionaries has {} as their persisted shape.
# MAGIC
# MAGIC

# COMMAND ----------

import dlt

@dlt.table (
  name = "magnusp_catalog.training_raw.my_table_from_test"
)
def func():
  return spark.readStream.table("carpartlifespan")

# COMMAND ----------

from pyspark.sql.types import array
@dlt.table (
  name = "my_table_2"
)
def func():
  # return array of maps
  return spark.sql("select * from values(array(map('a',2,'c',1 ), map('b',3,'d',4)))")

# COMMAND ----------

# MAGIC %md
# MAGIC We got an error in the session when we tried doing a group by.
# MAGIC This is normal if you do it on a dataframe, but not normal if you do it directly on the dataframe. dataframe.max only supports numerical fields, however the function max accepts max of string as with SQL.
# MAGIC
# MAGIC
# MAGIC Below you have the corrected syntax

# COMMAND ----------

from pyspark.sql.functions import max
@dlt.view(name="max_component")
def func():
  df =  spark.readStream.table("magnusp_catalog.training_raw.my_table_from_test")
  return df.groupBy().agg(max("component").alias("max_component"))