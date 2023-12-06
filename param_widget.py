# Databricks notebook source
dbutils.widgets.text("yr","2020","yearValue");


# COMMAND ----------

yr=dbutils.widgets.get("yr");
print (" year value is ",yr )

# COMMAND ----------

dbutils.jobs.taskValues.set(key = 'name', value = 'Ali');


# COMMAND ----------


