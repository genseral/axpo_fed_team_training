# Databricks notebook source
# MAGIC %md
# MAGIC # Dimensions

# COMMAND ----------

import dlt
from pyspark.sql.functions import col,lit, expr

dlt.create_streaming_table("gold_dim_customer")

@dlt.view()
def extract_customer_data():
  return spark.readStream.table("silver_customer_plan_and_events").select (
    "customer_id",
    "customer_name",
    "email",
    "customer_ingestion_ts",
    lit("I").alias("operation")
  )

dlt.apply_changes(
  target = "gold_dim_customer",
  source = "extract_customer_data",
  keys = ["customer_id"],
  sequence_by = col("customer_ingestion_ts"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  apply_as_truncates = expr("operation = 'TRUNCATE'"),
  except_column_list = ["operation", "customer_ingestion_ts"],
  stored_as_scd_type = 1
)

# COMMAND ----------

import dlt
from pyspark.sql.functions import col,lit, expr

dlt.create_streaming_table("gold_dim_plan")

@dlt.view()
def extract_plan_data():
  return spark.readStream.table("silver_customer_plan_and_events").select (
    "plan_id",
    "plan_name",
    "plan_cost_per_mb",
    "plan_cost_per_minute",
    "plan_ld_cost_per_minute",
    "plan_intl_cost_per_minute",
    "plan_ingestion_ts",
    lit("I").alias("operation")
  )

dlt.apply_changes(
  target = "gold_dim_plan",
  source = "extract_plan_data",
  keys = ["plan_id"],
  sequence_by = col("plan_ingestion_ts"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  apply_as_truncates = expr("operation = 'TRUNCATE'"),
  except_column_list = ["operation", "plan_ingestion_ts"],
  stored_as_scd_type = 1
)