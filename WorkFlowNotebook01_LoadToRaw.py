# Databricks notebook source
# MAGIC %md  
# MAGIC ### Workflow Demo
# MAGIC
# MAGIC #### - Be sure to upload the dimsalesterritory.csv to /FileStore/tables/ on dbfs before running this notebook.

# COMMAND ----------

# MAGIC %md 
# MAGIC Documentaion on using widgets: https://learn.microsoft.com/en-us/azure/databricks/notebooks/widgets

# COMMAND ----------

dbutils.widgets.dropdown("environment", "Dev", ["Dev", "Test", "Staging", "Prod"])

# COMMAND ----------

env = dbutils.widgets.get("environment")
print(f'This job is runing in the {env} environment.')

# COMMAND ----------

print('Workflow Notbook 01 - Load to Raw')

# COMMAND ----------

# MAGIC %fs ls '/Volumes/landing/ext/bronze/'

# COMMAND ----------

# DBTITLE 1,Create the schema on read source table is not already created
df=spark.read.csv("dbfs:/Volumes/landing/ext/bronze/DimSalesTerritory.csv",header=True,inferSchema=True);
df.write.mode('overwrite').format('delta').saveAsTable('DimSalesTerritory')

# COMMAND ----------

# DBTITLE 1,Get the data from the CSV file
spdf_dimsalesterritory = spark.sql('''select * from dimsalesterritory''')

# COMMAND ----------

# DBTITLE 1,Save the data to the raw layer overwriting existing table
spdf_dimsalesterritory.write.mode("overwrite").saveAsTable("raw_dimsalesterritory")

# COMMAND ----------

# DBTITLE 1,Verify the raw table has data
# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED raw_dimsalesterritory
