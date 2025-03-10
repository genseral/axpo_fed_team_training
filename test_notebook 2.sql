-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Collecting the SQL 1 statement pipelines in a notebook
-- MAGIC In this notebook I just want to show that the 1 statement DLT pipelines, easily can be added to a regular DLT pipeline with multiple steps, and be part of a larger end-to-end pipeline
-- MAGIC
-- MAGIC Create a ETL Pipeline and include test_notebook and test_notebook 2 and pres validate and you will see the end to end pipeline.
-- MAGIC If you do not provide the catalog and schema in the name, it will be installed in the target schema for the pipeline. If that schema does not exist, it will automatically be created.

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE CarPartLifespan
SCHEDULE REFRESH EVERY 8 WEEK
COMMENT 'This table is used to track the lifespan of car parts'
TBLPROPERTIES ('pipelines.channel' = 'preview')
AS
SELECT 
  Component
, Description
, `Typical Material` as Material
, `Estimated Lifespan` as lifespan
from STREAM read_files (
  '/Volumes/magnusp_catalog/training/source/*.csv'
, format => 'csv'
, header => true
, mode => 'FAILFAST'
)
GROUP BY ALL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Extended SQL
-- MAGIC As I mentioned we have an extended SQL in order to be able to handle deeply nested structures, (maps, arrays, structs), that could of course be embedded in arrays, maps, and structs.
-- MAGIC The way you do this is by using higher_order functions such as:
-- MAGIC * filter
-- MAGIC * flatten
-- MAGIC * transform
-- MAGIC * reverse
-- MAGIC * array_sort
-- MAGIC
-- MAGIC and then ways to turn the complex structure into rows through for instance <b>explode</b> and <b>posexplode</b>

-- COMMAND ----------


CREATE OR REPLACE MATERIALIZED VIEW magnusp_catalog.training.Lifespan_And_Components_V2
AS
WITH T1 AS (
  SELECT
    replace (replace(replace(lifespan, " ", "<SPC>"), "-", "<DASH>"),",", "<COMMA>") as lifespan_split_space
  , component
  FROM CarPartLifespan
),
T2 (
  SELECT 
    filter (
      flatten (
        transform (
          split(lifespan_split_space, "<SPC>"),  -- INPUT ARRAY
            x -> split ( -- FOR EACH ELEMENT IN ARRAY SPLIT
              replace (
                x, -- FIELD
                "<COMMA>", -- WHAT TO REPLACE
                "" -- REPLACE WITH EMPTY
              ), 
              "<DASH>" -- FIELD TO SPLIT ON
            )
        )
      ),  
      x-> lower(x) NOT IN (
        "", 
        "of",
        "the", 
        "(belt)", 
        "with", 
        "each", 
        "change", 
        "lifetime", 
        "replace"
      ) 
    )  as lifespan_split_space 
  , component
  FROM t1
), T3 (
  SELECT 
    upper(slice(reverse(lifespan_split_space),1, 1)[0]) as unit
  , reverse(slice(reverse(lifespan_split_space),2, size(lifespan_split_space)-1)) as rest
  , array_sort(collect_set(lower(component))) as components
  FROM T2
  GROUP BY 1,2
) , T4 (
  SELECT 
    unit
  , CAST(rest[0] as INT) as lifespan_min
  , CAST(rest[1] AS INT) as lifespan_max 
  , posexplode(components) (component_pos, component)
  FROM T3
)
SELECT * FROM T4