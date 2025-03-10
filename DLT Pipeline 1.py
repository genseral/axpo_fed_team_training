# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to DLT
# MAGIC DLT works with three types of Datasets:
# MAGIC * Streaming Tables (Permanent / Temporary) used to append data source, incremental data
# MAGIC * Materialized Views - Used for transformations, aggregations or computations
# MAGIC * Views - used for intermediate transformations, not stored in the target schema

# COMMAND ----------


import dlt
from pyspark.sql.functions import current_timestamp
# Streaming table due to spark.readStream
# using cloud files to read from a location, please observe that the location need to change in your code. So repoint schemaLocation, and load statement to the right location for the CSV files.

@dlt.table(
  name="bronze_plans",
  table_properties = { "quality": "bronze"},
  comment = "Plans bronze table"
)
def func():
  return spark.readStream.format("cloudfiles")\
    .option("cloudFiles.format", "json")\
    .option("cloudFiles.schemaLocation", "/Volumes/magnusp_catalog/training/source/schemas/plans")\
    .option("cloudFiles.schemaHints", """
            plan_id integer,
            plan_name string,
            cost_per_mb decimal(5,3), 
            cost_per_message decimal(5,3), 
            cost_per_minute decimal(5,3),
            ld_cost_per_minute decimal(5,3),
            intl_cost_per_minute decimal(5,3)
    """)\
    .option("cloudFiles.schemaEvolutionMode", "none")\
    .load (
      "/Volumes/magnusp_catalog/training/source/plans.json"
    ).withColumn (
      "plan_ingestion_ts", current_timestamp()
    ).select (
      col("plan_id"),
      col("plan_name"),
      col("cost_per_mb").alias("plan_cost_per_mb"),
      col("cost_per_message").alias("plan_cost_per_message"),
      col("cost_per_minute").alias("plan_cost_per_minute"),
      col("ld_cost_per_minute").alias("plan_ld_cost_per_minute"),
      col("intl_cost_per_minute").alias("plan_intl_cost_per_minute"),
      col("plan_ingestion_ts")
    )

# COMMAND ----------

# DBTITLE 1,JOIN
import dlt
from pyspark.sql.functions import expr

# TABLE (Materialized in target as a silver table)
@dlt.table(
  name="silver_customer_plan_and_events",
  table_properties = { "quality": "silver"}
)
def func():
  df        = spark.read.table("bronze_customers").alias("c")
  df_plans  = spark.read.table("bronze_plans").alias("p")
  df_events = spark.readStream.option("skipChangeCommits","true").table("bronze_device_events").alias("e").withWatermark("event_ingestion_ts", "30 minutes")
  df_joined = df.join(df_plans,on=expr("c.plan = p.plan_id"))
  df_joined_events = df_joined.join(df_events, how="inner",on=["device_id"])
  return df_joined_events

  # please explain these joins
  # what does skipChangeCommits do?
  # What is the withWatermark do?
  # Is all joins complete? or do we produce a cartesian product, or have the risk of creating a cartesian product. And what can we do to mitigate that risk?
  

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp
# Materialized view 
# please observe that the table path need to change to your catalog and to the schema you have for raw events.

@dlt.table(
  name="bronze_device_events",
  table_properties = { "quality": "bronze"},
  comment = "Events bronze table"
)
def func():
  df = spark.read.table("magnusp_catalog.training_raw.raw_events").withColumn("event_ingestion_ts", current_timestamp())
  return df

# why do we add and ingestion timestamp?

# COMMAND ----------

import dlt
from pyspark.sql.functions import count,sum,col,lower

@dlt.table(
  name = "gold_sms_costs",
  table_properties = { "quality": "gold"},
  comment = "Cube costs SMS"
)
def func():
  df = spark.read.table("silver_customer_plan_and_events")
  df_sms = df.where(
    lower(col("event_type")) == "sms"
  )
  df_sms_cnt = df_sms.groupBy(
    "customer_id",
    "phone_number", 
    "device_id", 
    "plan_cost_per_message"
  ).agg(
    count("event_ts").alias("sms_cnt")
  )
  df_sms_rollup = df_sms_cnt.rollup(
    "customer_id",
    "phone_number", 
    "device_id"
  ).agg(
    sum(
      col("sms_cnt") * col("plan_cost_per_message")
    ).alias("total_cost")
  )
  return df_sms_rollup

# COMMAND ----------

import dlt
from pyspark.sql.functions import count,sum,col,lower

@dlt.table(
  name = "gold_internet_costs",
  table_properties = { "quality": "gold"},
  comment = "Cube costs Internet"
)
def func():
  df = spark.read.table("silver_customer_plan_and_events")
  df_internet = df.where(
    lower(col("event_type")) == "internet"
  )
  df_internet_mb = df_internet.groupBy(
    "customer_id",
    "phone_number", 
    "device_id", 
    "plan_cost_per_mb"
  ).agg(
    sum("bytes_transferred").alias("bytes_transferred")
  )
  df_internet_rollup = df_internet_mb.rollup(
    "customer_id",
    "phone_number", 
    "device_id"
  ).agg(
    sum(
      col("bytes_transferred") * col("plan_cost_per_mb")
    ).alias("total_cost")
  )
  return df_internet_rollup

# COMMAND ----------

# Why do we not see this manifested as a table?
# what does it do?
# why do we use the col function?
import dlt
from pyspark.sql.functions import col

@dlt.view()
def gold_customers_sms_view():
  df = spark.read.table("gold_sms_costs").where(
    col("customer_id").isNotNull() & 
    col("phone_number").isNull() & 
    col("device_id").isNull() 
  )
  return df
  

# COMMAND ----------

# Why do we not see this manifested as a table?
# what does it do?
# why do we use the col function?

import dlt
from pyspark.sql.functions import col

@dlt.view()
def gold_customers_internet_view():
  df = spark.read.table("gold_internet_costs").where(
    col("customer_id").isNotNull() &
    col("phone_number").isNull() &
    col("device_id").isNull()
  )
  return df

# COMMAND ----------

# What is union doing, and why do we need to union? 
# can it be simplified?
# is this a streaming table or a materialized view, and how can you tell?

import dlt
from pyspark.sql.functions import sum

@dlt.table(
  name = "gold_customer_cost",
  table_properties = { "quality": "gold"},
  comment = "Summary of costs per customer"
)
def gold_customer_cost():
  df = spark.read.table("gold_customers_sms_view")
  df2 = spark.read.table("gold_customers_internet_view")
  df3 = spark.read.table("gold_customer_intl_call_view")
  df4 = spark.read.table("gold_customer_call_id_view")
  return df.union(df2).union(df3).union(df4).groupBy("customer_id").agg(sum("total_cost").alias("total_cost")).drop("device_id","phone_number")