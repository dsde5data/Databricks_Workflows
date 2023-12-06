# Databricks notebook source
# MAGIC %md  
# MAGIC ### Just a short demo notebook

# COMMAND ----------

print('Workflow Notbook 02 - MergeToTransformed')

# COMMAND ----------

# DBTITLE 1,Got starting code here but had to fix bugs
# MAGIC %md
# MAGIC
# MAGIC #### To check if the table exists. Copied code from blog below and fixed the bugs.
# MAGIC
# MAGIC https://kb.databricks.com/en_US/delta/programmatically-determine-if-a-table-is-a-delta-table-or-not

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC def delta_check(TableName: str) -> bool:
# MAGIC   try:
# MAGIC     desc_table = spark.sql(f"describe formatted {TableName}").collect()
# MAGIC     location = [i[1] for i in desc_table if i[0] == 'Location'][0]
# MAGIC     dir_check = dbutils.fs.ls(f"{location}/_delta_log")
# MAGIC     is_delta = True
# MAGIC   except Exception as e:
# MAGIC     is_delta = False
# MAGIC   return is_delta
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM raw_dimsalesterritory

# COMMAND ----------

# DBTITLE 1,If transformed table exists, do a merge, otherwise just copy the raw to the transformed
trans_table_exists = delta_check("transformed_dimsalesterritory")

if (trans_table_exists == True):
  print('transformed table does exist, will merge raw table.')
else:
  print('transformed table does not exist, copying raw table.')
  spark.sql('''CREATE TABLE IF NOT EXISTS transformed_dimsalesterritory AS SELECT * FROM raw_dimsalesterritory''')
  dbutils.notebook.exit('First load complete. Notebook stopped')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO transformed_dimsalesterritory transformed
# MAGIC USING raw_dimsalesterritory            raw
# MAGIC ON raw.SalesTerritoryKey = transformed.SalesTerritoryKey
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM transformed_dimsalesterritory limit 3
