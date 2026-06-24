# Databricks notebook source
# MAGIC %md
# MAGIC # CREATE FACT TABLE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading silver data

# COMMAND ----------

df_silver = spark.sql("SELECT * FROM parquet.`abfss://silver@carnithidatalake.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### # ### Reading the DIMS

# COMMAND ----------

df_dealer = spark.sql("Select * from cars_catalog.gold.dim_dealer")

df_branch = spark.sql("Select * from cars_catalog.gold.dim_branch")

df_date = spark.sql("Select * from cars_catalog.gold.dim_date")

df_model = spark.sql("Select * from cars_catalog.gold.dim_model")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bringing Keys to the fact table

# COMMAND ----------

df_fact = df_silver.join(df_branch, df_silver["Branch_ID"]==df_branch["Branch_Id"],how='left')\
                .join(df_dealer, df_silver["dealer_id"]==df_dealer["dealer_id"],how='left')\
                .join(df_date, df_silver["date_ID"]==df_date["date_ID"],how='left')\
                .join(df_model, df_silver["model_ID"]==df_model["model_ID"],how='left')\
                .select(df_silver['REVENUE'],df_silver['UNITS_SOLD'],df_silver['RevPerUnit'],df_branch['dim_branch_key'],df_dealer['dim_dealer_key'],df_model['dim_model_key'],df_date['dim_date_key'])
                

# COMMAND ----------

df_fact.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('factsales'):
    deltatbl = DeltaTable.forName(spark,'cars_catalog.gold.factsales')

    deltatbl.alias('trg').merge(df_fact.alias('src'),'trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key and trg.dim_model_key = src.dim_model_key and trg.dim_date_key = src.dim_date_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    df_fact.write.format("delta")\
        .mode("Overwrite")\
        .option("path","abfss://gold@carnithidatalake.dfs.core.windows.net/factsales")\
        .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from cars_catalog.gold.factsales

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model

# COMMAND ----------

