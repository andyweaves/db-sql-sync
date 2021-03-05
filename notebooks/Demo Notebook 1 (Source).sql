-- Databricks notebook source
-- MAGIC %python
-- MAGIC current_user = sql("SELECT current_user()").head()[0]
-- MAGIC current_user

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import time
-- MAGIC 
-- MAGIC epoch_time = int(time.time())
-- MAGIC epoch_time

-- COMMAND ----------

-- MAGIC %python
-- MAGIC database_name = "{}_{}".format(current_user.replace("@", "_").replace(".", "_"), epoch_time)
-- MAGIC database_name

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SHOW DATABASES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import *
-- MAGIC 
-- MAGIC df = spark.range(1, 1001).withColumn("partition_id", spark_partition_id())
-- MAGIC 
-- MAGIC @pandas_udf("id long, partition_id int, name string, email string, address string, credit_card string, expiry_date string, security_code string", PandasUDFType.GROUPED_MAP)
-- MAGIC def generate_fake_data(x):
-- MAGIC   
-- MAGIC   from faker import Faker
-- MAGIC   import pandas as pd
-- MAGIC   
-- MAGIC   def generate_data(y):
-- MAGIC       
-- MAGIC     from faker import Faker
-- MAGIC     fake = Faker('en_US')
-- MAGIC 
-- MAGIC     y["name"] = fake.name()
-- MAGIC     y["email"] = fake.email()
-- MAGIC     y["address"] = fake.address()
-- MAGIC     y["credit_card"] = fake.credit_card_number()
-- MAGIC     y["expiry_date"] = fake.credit_card_expire()
-- MAGIC     y["security_code"] = fake.credit_card_security_code()
-- MAGIC 
-- MAGIC     return y
-- MAGIC     
-- MAGIC   return x.apply(generate_data, axis=1)
-- MAGIC 
-- MAGIC df = df.groupBy("partition_id").apply(generate_fake_data).drop("partition_id").orderBy(asc("id"))
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df.write.mode("overwrite").option("path", f"abfss://customers@aweaveradlsgen2.dfs.core.windows.net/delta/{database_name}/").saveAsTable(f"{database_name}.customer_data")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SHOW TABLES IN {database_name}"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SHOW CREATE TABLE {database_name}.customer_data"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql(f"GRANT SELECT ON TABLE {database_name}.customer_data TO support")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SHOW GRANT ON DATABASE {database_name}"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SHOW GRANT ON TABLE {database_name}.customer_data"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql(f"""
-- MAGIC CREATE OR REPLACE VIEW {database_name}.customer_data_redacted 
-- MAGIC AS
-- MAGIC SELECT
-- MAGIC   id,
-- MAGIC   name,
-- MAGIC   address,
-- MAGIC   CASE WHEN
-- MAGIC     is_member('support') THEN credit_card
-- MAGIC     ELSE concat('XXXXXXXXXXXXXXXX', substr(credit_card, -3, 3))
-- MAGIC   END AS credit_card,
-- MAGIC   CASE WHEN
-- MAGIC     is_member('support') THEN expiry_date
-- MAGIC     ELSE regexp_replace(expiry_date, '^(0[1-9]|1[0-2])', 'XX')
-- MAGIC   END AS expiry_date,
-- MAGIC   CASE WHEN
-- MAGIC     is_member('support') THEN email
-- MAGIC     ELSE regexp_extract(email, '^.*@(.*)$', 1)
-- MAGIC   END AS email,
-- MAGIC   CASE WHEN
-- MAGIC     is_member('support') THEN security_code
-- MAGIC     ELSE 'XXX'
-- MAGIC   END AS security_code
-- MAGIC FROM {database_name}.customer_data
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(sql(f"SHOW TABLES IN {database_name}"))
