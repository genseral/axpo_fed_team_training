-- Databricks notebook source
USE magnusp_catalog.training_raw;

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
;

-- COMMAND ----------

SELECT * FROM CarPartLifespan

-- COMMAND ----------

USE magnusp_catalog.training_raw;
CREATE OR REFRESH STREAMING TABLE CarPartLifespan3 (
  Component STRING,
  Description STRING MASK magnusp_catalog.row_col_governance.general_mask_string using columns (
    'magnusp_catalog', 'training', 'carpartlifespan3','Description'
    ),
  Material STRING MASK magnusp_catalog.row_col_governance.general_mask_string using columns (
    'magnusp_catalog', 'training', 'carpartlifespan3','Material'
    ),
  lifespan STRING,
  CONSTRAINT CarPartLifespan3_pk PRIMARY KEY (Component, Description,Material) RELY
)
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
;

-- COMMAND ----------

CREATE OR REPLACE MATERIALIZED VIEW magnusp_catalog.training.Lifespan_And_Components
AS
WITH T1 AS (
  SELECT
    CASE  
      WHEN (lifespan) like '% years' THEN 'YEARS'
      WHEN (lifespan) like '% months' THEN 'MONTHS'
      WHEN lower(lifespan) like '% miles%' THEN 'MILES'
      WHEN lower(lifespan) like '% oil change%' THEN 'OIL'
      WHEN lower(lifespan) like '%lifetime%' THEN 'VEHICLE'
      ELSE 'OTHER'
      END as unit
  , if (
        contains(lower(lifespan), "-"), 
          cast(replace(trim(split(lower(lifespan),"-")[0]), ",", "") as INT), 
        if(
            contains(lower(lifespan),"miles") = true OR contains(lower(lifespan),"years") = true, 
              cast(replace(trim(split(lower(lifespan)," ")[0]), ",", "") as INT),
            NULL
        )) as lifespan_min
  ,  if (
          contains(lower(lifespan), "-"), 
            cast(replace(split(trim(split(lower(lifespan),"-")[1]), " ")[0], ",", "") as int), NULL
    ) as lifespan_max
  , array_sort(collect_set(lower(component))) as components 
  FROM 
    magnusp_catalog.training_raw.CarPartLifespan
  GROUP BY 1,2,3
  ORDER BY 1,2
)
  SELECT 
    * except(components)
  , posexplode(components) as (component_pos, component) 
  FROM T1
;

-- COMMAND ----------

USE magnusp_catalog.training;
CREATE OR REPLACE MATERIALIZED VIEW magnusp_catalog.training.Lifespan_And_Components_V2
AS
WITH T1 AS (
  SELECT
    replace (
      replace ( 
        replace (
          lifespan, 
          " ", 
          "<SPC>"
        ), 
        "-", 
        "<DASH>"
      ),
      ",", 
      "<COMMA>"
    ) as lifespan_split_space
  , component
  FROM carpartlifespan3
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

-- COMMAND ----------

SELECT * FROM magnusp_catalog.training.Lifespan_And_Components_V2
ORDER BY 1,2,3,4;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Functions introduced:
-- MAGIC * posexplode
-- MAGIC * array_sort
-- MAGIC * reverse
-- MAGIC * transform
-- MAGIC * split
-- MAGIC * slice
-- MAGIC * size
-- MAGIC * contains
-- MAGIC * filter
-- MAGIC * collect_set
-- MAGIC * flatten
-- MAGIC * cast
-- MAGIC
-- MAGIC Summary:
-- MAGIC Databricks has lots of functions not available in other languages. The reason for this is since we are not working with well prepared sets, we support nested structures, and can process in structure very efficiently

-- COMMAND ----------

-- DBTITLE 1,POSEXPLODE
-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC from pyspark.sql.functions import posexplode
-- MAGIC
-- MAGIC df = spark.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])
-- MAGIC display(df.select(posexplode(df.intlist)))
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,POSEXPLODE OUTER
-- MAGIC %python
-- MAGIC from pyspark.sql import Row
-- MAGIC from pyspark.sql.functions import posexplode_outer
-- MAGIC
-- MAGIC df = spark.createDataFrame(
-- MAGIC     [(1, ["foo", "bar"], {"x": 1.0}), (2, [], {}), (3, None, None)],
-- MAGIC     ("id", "an_array", "a_map")
-- MAGIC )
-- MAGIC display(df.select("id", "an_array", posexplode_outer("a_map")))
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,ARRAY_SORT
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import lit, array_sort, length, when
-- MAGIC df = spark.createDataFrame([([2, 1, None, 3],),([1],),([],)], ['data'])
-- MAGIC df.select(array_sort(df.data).alias('r')).collect()
-- MAGIC
-- MAGIC df = spark.createDataFrame([(["foo", "foobar", None, "bar"],),(["foo"],),([],)], ['data'])
-- MAGIC df2 = df.select(array_sort(
-- MAGIC     "data",
-- MAGIC     lambda x, y: when(x.isNull() | y.isNull(), lit(0)).otherwise(length(y) - length(x))
-- MAGIC ).alias("r"))
-- MAGIC display(df2)
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,REVERSE
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import reverse
-- MAGIC df = spark.createDataFrame([('Spark SQL',)], ['data'])
-- MAGIC display(df.select(reverse(df.data).alias('s')));
-- MAGIC
-- MAGIC df = spark.createDataFrame([([2, 1, 3,14],) ,([1,2],) ,([],)], ['data'])
-- MAGIC display(df.select(reverse(df.data).alias('r')));
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,TRANSFORM
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import transform
-- MAGIC
-- MAGIC df = spark.createDataFrame([(1, [1, 2, 3, 4])], ("key", "values"))
-- MAGIC display(df.select(transform("values", lambda x: x * 2).alias("doubled")))
-- MAGIC
-- MAGIC # Alternatively you can define your own python function and pass in:
-- MAGIC
-- MAGIC def alternate(x, i):
-- MAGIC     return when(i % 2 == 0, x).otherwise(-x)
-- MAGIC
-- MAGIC display(df.select(transform("values", alternate).alias("alternated")))
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,SPLIT
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import split
-- MAGIC df = spark.createDataFrame([('oneAtwoBthreeCfour',)], ['s',])
-- MAGIC display(df.select(split(df.s, '[ABC]', 3).alias('s')))
-- MAGIC
-- MAGIC display(df.select(split(df.s, '[ABC]', -1).alias('s')))
-- MAGIC display(df.select(split(df.s, '[ABC]').alias('s')))
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,SLICE
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import slice
-- MAGIC
-- MAGIC df = spark.createDataFrame([([1, 2, 3],), ([4, 5],)], ['x'])
-- MAGIC df2 = df.select(slice(df.x, 2, 2).alias("sliced"))
-- MAGIC display(df2)
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,SIZE
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import size
-- MAGIC
-- MAGIC df = spark.createDataFrame([
-- MAGIC   ([1, 2, 3],),
-- MAGIC   ([1],),
-- MAGIC   ([],),
-- MAGIC   (None,)],
-- MAGIC   ['data']
-- MAGIC )
-- MAGIC display(df.select(size(df.data)))
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,CONTAINS
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import contains
-- MAGIC df = spark.createDataFrame([("Spark SQL", "Spark")], ['a', 'b'])
-- MAGIC display(df.select(contains(df.a, df.b).alias('r')))
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,FILTER
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import filter, month, to_date
-- MAGIC
-- MAGIC df = spark.createDataFrame(
-- MAGIC     [(1, ["2018-09-20",  "2019-02-03", "2019-07-01", "2020-06-01", "2020-09-01"])],
-- MAGIC     ("key", "values")
-- MAGIC )
-- MAGIC def after_second_quarter(x):
-- MAGIC     return month(to_date(x)) > 6
-- MAGIC
-- MAGIC df2 = df.select(
-- MAGIC     filter("values", after_second_quarter).alias("after_second_quarter")
-- MAGIC )
-- MAGIC
-- MAGIC display(df2)
-- MAGIC df2.printSchema()
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,COLLECT_SET
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import collect_set
-- MAGIC df2 = spark.createDataFrame([(2,), (5,), (5,)], ('age',))
-- MAGIC display(df2.agg(array_sort(collect_set('age')).alias('c')))
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,FLATTEN
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import flatten, array_compact
-- MAGIC df = spark.createDataFrame([
-- MAGIC   ([[1, 2, 3], [4, 5], [6]],), 
-- MAGIC   ([None, [4, 5]],)], ['data'])
-- MAGIC # Pre processing
-- MAGIC display(df)
-- MAGIC
-- MAGIC # flatten the array into one array
-- MAGIC display(df.select(flatten(df.data).alias('r')))
-- MAGIC
-- MAGIC # array_compact removes null values from array
-- MAGIC display(df.select(flatten(array_compact(df.data).alias(''))))
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Column.Cast
-- MAGIC %python
-- MAGIC from pyspark.sql.types import StringType
-- MAGIC df = spark.createDataFrame(
-- MAGIC      [(2, "Alice"), (5, "Bob")], ["age", "name"])
-- MAGIC display(df.select(df.age.cast("string").alias('ages')))
-- MAGIC
-- MAGIC display(df.select(df.age.cast(StringType()).alias('ages')))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.explain(True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.explain("extended")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("select * from magnusp_catalog.training.lifespan_and_components_v2")
-- MAGIC df.explain(True)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.explain("cost")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.explain("codegen")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC x = spark.read.format("delta").load("/Volumes/magnusp_catalog/database1/sources/helly_test")

-- COMMAND ----------

-- DBTITLE 1,Need more work
-- MAGIC %python
-- MAGIC from typing import Iterator
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.functions import col,lit
-- MAGIC
-- MAGIC def renormalize(itr) -> Iterator[pd.DataFrame]:
-- MAGIC     for df in itr:
-- MAGIC         x = df.explode("attribs")
-- MAGIC         yield pd.DataFrame( {'name': x['_name'], 'default': x['_default']})
-- MAGIC             
-- MAGIC df = x.select(col('xs:complexType.xs:attribute').alias("attribs"))
-- MAGIC #expected_schema = 'name string, default string'
-- MAGIC #df = df.mapInPandas(renormalize, expected_schema)
-- MAGIC
-- MAGIC pdf = df.toPandas().transpose()
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,JOINS
-- MAGIC %python
-- MAGIC x = spark.read.table("magnusp_catalog.training.lifespan_and_components_v2").alias("x") 
-- MAGIC y = spark.read.table("magnusp_catalog.training.lifespan_and_components").alias("y")
-- MAGIC
-- MAGIC x2 = x.subtract(y)
-- MAGIC display(x2)
-- MAGIC x3 = y.subtract(x)
-- MAGIC display(x3)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(x2)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC x = spark.read.table("magnusp_catalog.training.lifespan_and_components_v2").alias("x") 
-- MAGIC y = spark.read.table("magnusp_catalog.training.lifespan_and_components").alias("y")
-- MAGIC
-- MAGIC x2 = x.join(y, ["unit", "component", "component_pos"], "left_anti")
-- MAGIC display(x2)
-- MAGIC
-- MAGIC x3 = x.join(y, ["unit", "component", "component_pos"], "left")
-- MAGIC display(x3)
-- MAGIC
-- MAGIC x4 = x.join(y, ["unit", "component", "component_pos"], "right")
-- MAGIC display(x4)
-- MAGIC
-- MAGIC x5 = x.join(y, ["unit", "component", "component_pos"], "FULL")
-- MAGIC display(x5)
-- MAGIC x5.printSchema()
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,LATERAL VIEW

WITH T1 (
SELECT T2,T3,T4POS, T4
  LATERAL VIEW       EXPLODE(sequence(1, 100)) as T2
  LATERAL VIEW OUTER EXPLODE(shuffle(sequence(1000,1010))) as T3
  LATERAL VIEW       POSEXPLODE(shuffle(sequence(1000,1010))) as T4POS,T4
ORDER BY 1,2
)
SELECT * FROM T1 TABLESAMPLE(10 PERCENT)


-- COMMAND ----------

select unit,count(*) cnt from magnusp_catalog.training.lifespan_and_components_v2
GROUP BY 1
ORDER BY 2 DESC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC x = spark.read.table("magnusp_catalog.training.Lifespan_And_Components_V2")
-- MAGIC y = x.repartitionByRange(5,'unit')
-- MAGIC y.write.format("delta").mode("overwrite").save("/Volumes/magnusp_catalog/database1/sources/partitioned_set")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC y = spark.read.format("delta").load("/Volumes/magnusp_catalog/database1/sources/partitioned_set").select("*","_metadata.file_name")
-- MAGIC y1 = y.groupBy("file_name").count().orderBy("file_name")
-- MAGIC display(y1)
-- MAGIC