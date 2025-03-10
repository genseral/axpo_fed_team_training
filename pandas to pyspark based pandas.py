# Databricks notebook source
import pandas as pd
import pyspark.pandas as ps
import time 
import numpy as np
from pyspark.sql.functions import when


# COMMAND ----------

# MAGIC %md
# MAGIC ### The follow notebook shows the path from Pandas to PySpark Pandas to using Pyspark dataframes. All the three examples perform the sampe small data wrangling operations. Note the differences/simliarties in terms of synthax and also check out the warnings that point towards the fundamental differences between the three approaches.

# COMMAND ----------

# reading data as pandas df
pandas_df = pd.read_csv('/Volumes/algenser_test/training/source/customers.csv/part-00000-tid-991803081524073041-b820c942-2d57-4d19-9af4-af7ba12e04ec-3250-1-c000.csv')
display(pandas_df)

# COMMAND ----------

# some data wrangling with classic pandas operations
pandas_df['status'] = pandas_df['plan'].apply(lambda x: 'Premium' if x > 110 else 'Basic')
status_count = pandas_df['status'].value_counts().reset_index()
status_count.columns = ['status', 'count']
display(status_count)

# COMMAND ----------

# switching to pyspark pandas
psdf = ps.DataFrame(pandas_df)

# some data wrangling with pandas like API on top of spark
psdf['status'] = psdf['plan'].apply(lambda x: 'Premium' if x > 110 else 'Basic')
status_count = psdf['status'].value_counts().reset_index()
status_count.columns = ['status', 'count']
display(status_count)

# what is the warning regarding the index_col hinting here?

# COMMAND ----------

# create a pyspark dataframe from pandas df
pyspark_df = spark.createDataFrame(pandas_df)

# some data wrangling with pyspark
pyspark_df = pyspark_df.withColumn('status', when(pyspark_df['plan'] > 110, 'Premium').otherwise('Basic'))
status_count = pyspark_df.groupBy('status').count()
display(status_count)

# COMMAND ----------

# convert pyspark dataframe back to pyspark pandas
psdf_copy = pyspark_df.pandas_api()

# COMMAND ----------

#converting pyspark dataframe back to pandas
pandas_df_copy = pyspark_df.toPandas()
