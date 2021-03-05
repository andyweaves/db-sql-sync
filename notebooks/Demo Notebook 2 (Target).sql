-- Databricks notebook source
-- MAGIC %sql
-- MAGIC SHOW DATABASES LIKE 'andrew_weaver_databricks_com*' 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import desc
-- MAGIC 
-- MAGIC display(sql("SHOW DATABASES LIKE 'andrew_weaver_databricks_com*'").orderBy(desc("namespace")))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC database_name = sql("SHOW DATABASES LIKE 'andrew_weaver_databricks_com*'").orderBy(desc("namespace")).head()[0]
-- MAGIC database_name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"DESCRIBE DATABASE {database_name}"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SHOW GRANT ON DATABASE {database_name}"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SHOW TABLES IN {database_name}"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SHOW GRANT ON TABLE {database_name}.customer_data"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SELECT * FROM {database_name}.customer_data_redacted"))
