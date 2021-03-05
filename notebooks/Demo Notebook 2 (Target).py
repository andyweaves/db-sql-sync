# Databricks notebook source
current_user = sql("SELECT current_user()").head()[0].replace("@", "_").replace(".", "_")
current_user

# COMMAND ----------

from pyspark.sql.functions import desc

display(sql(f"SHOW DATABASES LIKE '{current_user}*'").orderBy(desc("namespace")))

# COMMAND ----------

database_name = sql("SHOW DATABASES LIKE 'andrew_weaver_databricks_com*'").orderBy(desc("namespace")).head()[0]
database_name

# COMMAND ----------

display(sql(f"DESCRIBE DATABASE {database_name}"))

# COMMAND ----------

display(sql(f"SHOW GRANT ON DATABASE {database_name}"))

# COMMAND ----------

display(sql(f"SHOW TABLES IN {database_name}"))

# COMMAND ----------

display(sql(f"SHOW GRANT ON TABLE {database_name}.customer_data"))

# COMMAND ----------

display(sql(f"SELECT * FROM {database_name}.customer_data_redacted"))
