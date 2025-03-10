# Databricks notebook source
# MAGIC %md
# MAGIC # Test data generation
# MAGIC This is what creates the test data

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# creating a widget to set catalog name; set your catalog name in the widget that appears in the top left or in the code
dbutils.widgets.text("catalog_name", "algenser_test")
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog_name}.training_test;

# COMMAND ----------

# DBTITLE 1,Create the volume
# MAGIC %sql
# MAGIC -- Please remember to change catalog to the catalog name you want
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog_name}.training;
# MAGIC CREATE VOLUME IF NOT EXISTS ${catalog_name}.training.source;
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog_name}.training_raw;
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog_name}.training_silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ${catalog_name}.training_gold;
# MAGIC

# COMMAND ----------

# DBTITLE 1,Genrate the plans
import dbldatagen as dg
import pyspark.sql.functions as F

# clear cache so that if we run multiple times to check performance,


UNIQUE_PLANS = 20
PLAN_MIN_VALUE = 100

shuffle_partitions_requested = 8
partitions_requested = 1
data_rows = UNIQUE_PLANS # we'll generate one row for each plan

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)


plan_dataspec = (
    dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
    .withColumn("plan_id","int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS)
    # use plan_id as root value
    .withColumn("plan_name", prefix="plan", baseColumn="plan_id")

    # note default step is 1 so you must specify a step for small number ranges,
    .withColumn("cost_per_mb", "decimal(5,3)", minValue=0.005, maxValue=0.050,
                step=0.005, random=True)
    .withColumn("cost_per_message", "decimal(5,3)", minValue=0.001, maxValue=0.02,
                step=0.001, random=True)
    .withColumn("cost_per_minute", "decimal(5,3)", minValue=0.001, maxValue=0.01,
                step=0.001, random=True)

    # we're modelling long distance and international prices simplistically -
    # each is a multiplier thats applied to base rate
    .withColumn("ld_multiplier", "decimal(5,3)", minValue=1.5, maxValue=3, step=0.05,
                random=True, distribution="normal", omit=True)
    .withColumn("ld_cost_per_minute", "decimal(5,3)",
                expr="cost_per_minute * ld_multiplier",
                baseColumns=['cost_per_minute', 'ld_multiplier'])
    .withColumn("intl_multiplier", "decimal(5,3)", minValue=2, maxValue=4, step=0.05,
                random=True,  distribution="normal", omit=True)
    .withColumn("intl_cost_per_minute", "decimal(5,3)",
                expr="cost_per_minute * intl_multiplier",
                baseColumns=['cost_per_minute', 'intl_multiplier'])
            )

df_plans = plan_dataspec.build().cache()

display(df_plans)

# COMMAND ----------

# DBTITLE 1,Store the plans in the volume
# You have to change this to point to the volume you created
volume_path = f"/Volumes/{catalog_name}/training/source"
df_plans.write.format("json").mode("overwrite").save(f"{volume_path}/plans.json")

# COMMAND ----------

# DBTITLE 1,Test read the file created
#create path /training/source on the volume
volume_path = f"/Volumes/{catalog_name}/training/source"
x = spark.read.format("json").load(f"{volume_path}/plans.json")
display(x)

# COMMAND ----------

# DBTITLE 1,Generate Customers
import dbldatagen as dg
import pyspark.sql.functions as F

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)

UNIQUE_CUSTOMERS = 50000
CUSTOMER_MIN_VALUE = 1000
DEVICE_MIN_VALUE = 1000000000
SUBSCRIBER_NUM_MIN_VALUE = 1000000000

spark.catalog.clearCache()  # clear cache so that if we run multiple times to check
                            # performance, we're not relying on cache
shuffle_partitions_requested = 8
partitions_requested = 8
data_rows = UNIQUE_CUSTOMERS

customer_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested)
            .withColumn("customer_id","decimal(10)", minValue=CUSTOMER_MIN_VALUE,
                        uniqueValues=UNIQUE_CUSTOMERS)
            .withColumn("customer_name", template=r"\\w \\w|\\w a. \\w")

            # use the following for a simple sequence
            #.withColumn("device_id","decimal(10)", minValue=DEVICE_MIN_VALUE,
            #              uniqueValues=UNIQUE_CUSTOMERS)

            .withColumn("device_id","decimal(10)",  minValue=DEVICE_MIN_VALUE,
                        baseColumn="customer_id", baseColumnType="hash")

            .withColumn("phone_number","decimal(10)",  minValue=SUBSCRIBER_NUM_MIN_VALUE,
                        baseColumn=["customer_id", "customer_name"], baseColumnType="hash")

            # for email, we'll just use the formatted phone number
            .withColumn("email","string",  format="subscriber_%s@myoperator.com",
                        baseColumn="phone_number")
            .withColumn("plan", "int", minValue=PLAN_MIN_VALUE, uniqueValues=UNIQUE_PLANS,
                        random=True)
            )

df_customers = (customer_dataspec.build()
                .dropDuplicates(["device_id"])
                .dropDuplicates(["phone_number"])
                .orderBy("customer_id")
                .cache()
               )

effective_customers = df_customers.count()


     


# COMMAND ----------

# DBTITLE 1,Write Customers to volume
volume_path = f"/Volumes/{catalog_name}/training/source"
df_customers.write.format("csv").option("header", "true").mode("overwrite").save(f"{volume_path}/customers.csv")

# COMMAND ----------

# DBTITLE 1,Create folder in volume for schemas
volume_path = f"/Volumes/{catalog_name}/training/source"
dbutils.fs.mkdirs(f"{volume_path}/schemas/plans")

# COMMAND ----------

import dbldatagen as dg
import pyspark.sql.functions as F

AVG_EVENTS_PER_CUSTOMER = 50

spark.catalog.clearCache()
shuffle_partitions_requested = 8
partitions_requested = 8
NUM_DAYS=31
MB_100 = 100 * 1000 * 1000
K_1 = 1000
data_rows = AVG_EVENTS_PER_CUSTOMER * UNIQUE_CUSTOMERS * NUM_DAYS

spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 20000)


# use random seed method of 'hash_fieldname' for better spread - default in later builds
events_dataspec = (dg.DataGenerator(spark, rows=data_rows, partitions=partitions_requested,
                   randomSeed=42, randomSeedMethod="hash_fieldname")
             # use same logic as per customers dataset to ensure matching keys
             # but make them random
            .withColumn("device_id_base","decimal(10)", minValue=CUSTOMER_MIN_VALUE,
                        uniqueValues=UNIQUE_CUSTOMERS,
                        random=True, omit=True)
            .withColumn("device_id","decimal(10)",  minValue=DEVICE_MIN_VALUE,
                        baseColumn="device_id_base", baseColumnType="hash")

            # use specific random seed to get better spread of values
            .withColumn("event_type","string",
                        values=[ "sms", "internet", "local call", "ld call", "intl call" ],
                        weights=[50, 50, 20, 10, 5 ], random=True)

            # use Gamma distribution for skew towards short calls
            .withColumn("base_minutes","decimal(7,2)",
                        minValue=1.0, maxValue=100.0, step=0.1,
                        distribution=dg.distributions.Gamma(shape=1.5, scale=2.0),
                        random=True, omit=True)

            # use Gamma distribution for skew towards short transfers
            .withColumn("base_bytes_transferred","decimal(12)",
                        minValue=K_1, maxValue=MB_100,
                        distribution=dg.distributions.Gamma(shape=0.75, scale=2.0),
                        random=True, omit=True)

            .withColumn("minutes", "decimal(7,2)",
                        baseColumn=["event_type", "base_minutes"],
                        expr= """
                              case when event_type in ("local call", "ld call", "intl call")
                                  then base_minutes
                                  else 0
                              end
                               """)
            .withColumn("bytes_transferred", "decimal(12)",
                        baseColumn=["event_type", "base_bytes_transferred"],
                        expr= """
                              case when event_type = "internet"
                                   then base_bytes_transferred
                                   else 0
                              end
                               """)

            .withColumn("event_ts", "timestamp",
                         data_range=dg.DateRange("2020-07-01 00:00:00",
                                                 "2020-07-31 11:59:59",
                                                 "seconds=1"),
                        random=True)

            )

df_events = events_dataspec.build()


# COMMAND ----------

# DBTITLE 1,Generate the events as a table
df_events.write.mode("overwrite").saveAsTable(f"{catalog_name}.training_raw.events")
