# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # DATA READING

# COMMAND ----------

df = spark.read.format("parquet")\
          .option ("inferschema",True)\
          .load("abfss://bronze@carnithidatalake.dfs.core.windows.net/rawdata")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA TRANSFORMATION

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = df.withColumn('model_category',split(col('MODEL_ID'),'-').getItem(0))
df.display()

# COMMAND ----------

df.withColumn('UNITS_SOLD',col('UNITS_SOLD').cast("string")).printSchema()

# COMMAND ----------

df = df.withColumn('RevPerUnit',col('REVENUE')/col('UNITS_SOLD'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # AD-HOC

# COMMAND ----------

df.display()

# COMMAND ----------

display(df.groupBy('YEAR','BRANCHNAME').agg(sum("UNITS_SOLD").alias('Total_units')).sort('YEAR','Total_units',ascending=[1,0]))

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA WRITING

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .option('path','abfss://silver@carnithidatalake.dfs.core.windows.net/carsales')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC # Querying Silver Data

# COMMAND ----------

# MAGIC %sql
# MAGIC  SELECT * FROM parquet.`abfss://silver@carnithidatalake.dfs.core.windows.net/carsales`