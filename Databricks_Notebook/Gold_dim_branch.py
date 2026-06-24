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
SELECT DISTINCT (branch_id) Branch_ID, BranchName
FROM parquet.`abfss://silver@carnithidatalake.dfs.core.windows.net/carsales`
''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_model sink initial & Incremental (Just bring the schema if table NOT EXISTS)

# COMMAND ----------


if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):

   df_sink = spark.sql('''
   select dim_branch_key, branch_id, branchname
   from cars_catalog.gold.dim_branch
''')

else:
    df_sink = spark.sql('''
    select 1 as dim_branch_key, branch_id, branchname 
    from parquet.`abfss://silver@carnithidatalake.dfs.core.windows.net/carsales`
    where 1=0
''')


# COMMAND ----------

df_sink.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## filtering new records and old records

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src['branch_id'] == df_sink['branch_id'], 'left').select(df_src['branch_id'],df_src['branchname'],df_sink['dim_branch_key'])

# COMMAND ----------

df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_old
# MAGIC

# COMMAND ----------

df_filter_old = df_filter.filter(col('dim_branch_key').isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ### df_filter_new

# COMMAND ----------

df_filter_new = df_filter.filter(col('dim_branch_key').isNull()).select(col('branch_id'),(col('branchname')))

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Surrogate key

# COMMAND ----------

if (incremental_flag == '0'):
    max_value =1
else:
    max_value_df = spark.sql("select max(dim_branch_key) from cars_catalog.gold.dim_branch")
    max_value = max_value_df.collect()[0][0]

# COMMAND ----------

# MAGIC %md
# MAGIC ### create surrogate key column and ADD the max surrogate key

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_branch_key',max_value + monotonically_increasing_id())

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
if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_tbl = DeltaTable.forPath(spark,"abfss://gold@carnithidatalake.dfs.core.windows.net/dim_branch")

    delta_tbl.alias("trg").merge(df_final.alias("src"),"trg.dim_branch_key = src.dim_branch_key")\
                .whenMatchedUpdateAll()\
                .whenNotMatchedInsertAll()\
                .execute()
                 
#INITIAL RUN
else:
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path","abfss://gold@carnithidatalake.dfs.core.windows.net/dim_branch")\
        .saveAsTable("cars_catalog.gold.dim_branch")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_branch
