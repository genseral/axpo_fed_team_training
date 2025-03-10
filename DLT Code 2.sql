-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC This file is used in conjunction with the files: 
-- MAGIC * <b>DLT Pipeline 1</b>
-- MAGIC * <b>SCD's</b>
-- MAGIC
-- MAGIC
-- MAGIC to create a pipeline.
-- MAGIC

-- COMMAND ----------

-- Remember to change the source folder for the file
-- what is the difference or are there a difference between read_files, and cloud_files (autoloader)
CREATE OR REFRESH STREAMING TABLE bronze_customers
AS 
SELECT * except(_rescued_data), current_timestamp() as customer_ingestion_ts
FROM 
  STREAM read_files("/Volumes/magnusp_catalog/training/source/customers.csv", format => "csv", header => true, inferSchema => true, mode => "DROPMALFORMED")

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW gold_customer_call_id_costs
AS 
WITH T1 AS (
SELECT 
  customer_id
, phone_number
, device_id
, plan_ld_cost_per_minute
, sum(minutes) as sum_minutes 
FROM silver_customer_plan_and_events
WHERE lower(event_type) = 'ld call'
GROUP BY 1,2,3,4
)
SELECT 
  t0.customer_id
, t0.phone_number
, t0.device_id
, SUM(t0.sum_minutes * t0.plan_ld_cost_per_minute) as cost 
FROM T1 as t0
GROUP BY ROLLUP (t0.customer_id, t0.phone_number, t0.device_id )


-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW gold_customer_intl_call_costs
AS 
WITH T1 AS (
SELECT 
  customer_id
, phone_number
, device_id
, plan_intl_cost_per_minute
, sum(minutes) as sum_minutes 
FROM silver_customer_plan_and_events
WHERE lower(event_type) = 'intl call'
GROUP BY 1,2,3,4
)
SELECT 
  t0.customer_id
, t0.phone_number
, t0.device_id
, SUM(t0.sum_minutes * t0.plan_intl_cost_per_minute) as cost 
FROM T1 as t0
GROUP BY ROLLUP (t0.customer_id, t0.phone_number, t0.device_id )

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY MATERIALIZED VIEW gold_customer_intl_call_view
AS
SELECT * FROM gold_customer_intl_call_costs 
WHERE customer_id IS NOT NULL 
AND   phone_number IS NULL


-- COMMAND ----------

CREATE OR REPLACE TEMPORARY MATERIALIZED VIEW gold_customer_call_id_view
AS
SELECT * FROM gold_customer_call_id_costs 
WHERE customer_id IS NOT NULL 
AND   phone_number IS NULL