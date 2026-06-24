# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC # CREATE FLAG PARAMETER

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')

# COMMAND ----------

incremental_flag = dbutils.widgets.get('incremental_flag')


# COMMAND ----------

# MAGIC %md
# MAGIC # CREATING DIMENSION MODEL

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch relative column

# COMMAND ----------

df_src = spark.sql('''
SELECT DISTINCT (dealer_id) Dealer_ID, dealername
FROM parquet.`abfss://silver@carnithidatalake.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_model sink initial & Incremental (Just bring the schema if table NOT EXISTS)

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):

   df_sink = spark.sql('''
   select dim_dealer_key, dealer_id, dealername
   from cars_catalog.gold.dim_dealer
''')

else:
    df_sink = spark.sql('''
    select 1 as dim_dealer_key, dealer_id, dealername
    from parquet.`abfss://silver@carnithidatalake.dfs.core.windows.net/carsales`
    where 1=0
''')


# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['dealer_id'] == df_sink['dealer_id'], 'left').select(df_src['dealer_id'],df_src['dealername'],df_sink['dim_dealer_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old
# MAGIC

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_dealer_key').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_dealer_key').isNull()).select(col('dealer_id'),(col('dealername')))

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Surrogate key

# COMMAND ----------

if (incremental_flag == '0'):
    max_value =1
else:
    max_value_df = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### create surrogate key column and ADD the max surrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key',max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Final DF ---df_filter_old + df_filter_new

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD TYPE - 1(UPSERT)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

#Incremental Run
if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_tbl = DeltaTable.forPath(spark,"abfss://gold@carnithidatalake.dfs.core.windows.net/dim_dealer")

    delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_dealer_key = src.dim_dealer_key")\
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()
                 
#INITIAL RUN
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://gold@carnithidatalake.dfs.core.windows.net/dim_dealer")\
        .saveAsTable("cars_catalog.gold.dim_dealer")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer