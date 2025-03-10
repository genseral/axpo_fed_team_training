# Databricks notebook source
catalog_name = 'algenser_test'
schema_name = 'training_data'

# COMMAND ----------

x = spark.sql(""" 
  WITH T1 AS (
    SELECT 
    table_catalog
  , table_schema
  , table_name
  , "magnusp_catalog.training_bronze" as target_schema
  FROM magnusp_catalog.information_schema.tables
  WHERE table_name like "bronze_%" and table_schema = 'training'
  )
  SELECT 
    "CREATE OR REPLACE VIEW " || 
      target_schema || "." || 
      table_name || " AS " || 
      "SELECT * FROM " || 
      table_catalog || "." || 
      table_schema || "." || 
      table_name || ";" AS sql_script 
  FROM T1 
""").collect()
for i in x:
  spark.sql(i.sql_script)

# COMMAND ----------

x = spark.sql(""" 
  WITH T1 AS (
    SELECT 
    table_catalog
  , table_schema
  , table_name
  , "magnusp_catalog.training_silver" as target_schema
  FROM magnusp_catalog.information_schema.tables
  WHERE table_name like "silver_%" and table_schema = 'training'
  )
  SELECT 
    "CREATE OR REPLACE VIEW " || 
      target_schema || "." || 
      table_name || " AS " || 
      "SELECT * FROM " || 
      table_catalog || "." || 
      table_schema || "." || 
      table_name || ";" AS sql_script 
  FROM T1 
""").collect()
for i in x:
  spark.sql(i.sql_script)

# COMMAND ----------

x = spark.sql(""" 
  WITH T1 AS (
    SELECT 
    table_catalog
  , table_schema
  , table_name
  , "magnusp_catalog.training_gold" as target_schema
  FROM magnusp_catalog.information_schema.tables
  WHERE table_name like "gold_%" and table_schema = 'training'
  )
  SELECT 
    "CREATE OR REPLACE VIEW " || 
      target_schema || "." || 
      table_name || " AS " || 
      "SELECT * FROM " || 
      table_catalog || "." || 
      table_schema || "." || 
      table_name || ";" AS sql_script 
  FROM T1 
""").collect()
for i in x:
  spark.sql(i.sql_script)
