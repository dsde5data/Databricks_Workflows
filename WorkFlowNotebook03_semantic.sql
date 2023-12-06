-- Databricks notebook source
-- MAGIC %md  
-- MAGIC ### Refresh the Semantic tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print('Workflow Notbook 03 - Refresh Semantic layer')

-- COMMAND ----------

DROP TABLE IF EXISTS semantic_dimsalesterritory_summary

-- COMMAND ----------

CREATE OR REPLACE TABLE semantic_dimsalesterritory_summary
SELECT SalesTerritoryGroup, count(*) as STCount
FROM transformed_dimsalesterritory 
GROUP BY SalesTerritoryGroup

-- COMMAND ----------

SELECT * FROM semantic_dimsalesterritory_summary
