# Databricks notebook source
import pandas as pd
import pyspark.pandas as ps
df = pd.read_csv('/Volumes/algenser_test/training/source/customers.csv/part-00000-tid-991803081524073041-b820c942-2d57-4d19-9af4-af7ba12e04ec-3250-1-c000.csv')
print(df)
dct = df.to_dict(orient='records')
for i in dct:
  print(i['customer_id'], i['customer_name'], i['device_id'], i['phone_number'], i['email'], i['plan'])

df2 = ps.from_pandas(df)
display(df2)


# COMMAND ----------

