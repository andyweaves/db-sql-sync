# Databricks notebook source
current_user = sql("SELECT current_user()").head()[0]
current_user

# COMMAND ----------

import time

epoch_time = int(time.time())
epoch_time

# COMMAND ----------

database_name = "{}_{}".format(current_user.replace("@", "_").replace(".", "_"), epoch_time)
database_name

# COMMAND ----------

sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES

# COMMAND ----------

from pyspark.sql.functions import *

df = spark.range(1, 1001).withColumn("partition_id", spark_partition_id())

@pandas_udf("id long, partition_id int, name string, email string, address string, credit_card string, expiry_date string, security_code string", PandasUDFType.GROUPED_MAP)
def generate_fake_data(x):
  
  from faker import Faker
  import pandas as pd
  
  def generate_data(y):
      
    from faker import Faker
    fake = Faker('en_US')

    y["name"] = fake.name()
    y["email"] = fake.email()
    y["address"] = fake.address()
    y["credit_card"] = fake.credit_card_number()
    y["expiry_date"] = fake.credit_card_expire()
    y["security_code"] = fake.credit_card_security_code()

    return y
    
  return x.apply(generate_data, axis=1)

df = df.groupBy("partition_id").apply(generate_fake_data).drop("partition_id").orderBy(asc("id"))
display(df)

# COMMAND ----------

df.write.mode("overwrite").option("path", f"abfss://customers@aweaveradlsgen2.dfs.core.windows.net/delta/{database_name}/").saveAsTable(f"{database_name}.customer_data")

# COMMAND ----------

display(sql(f"SHOW TABLES IN {database_name}"))

# COMMAND ----------

display(sql(f"SHOW CREATE TABLE {database_name}.customer_data"))

# COMMAND ----------

sql(f"GRANT SELECT ON TABLE {database_name}.customer_data TO support")

# COMMAND ----------

display(sql(f"SHOW GRANT ON DATABASE {database_name}"))

# COMMAND ----------

display(sql(f"SHOW GRANT ON TABLE {database_name}.customer_data"))

# COMMAND ----------

sql(f"""
CREATE OR REPLACE VIEW {database_name}.customer_data_redacted 
AS
SELECT
  id,
  name,
  address,
  CASE WHEN
    is_member('support') THEN credit_card
    ELSE concat('XXXXXXXXXXXXXXXX', substr(credit_card, -3, 3))
  END AS credit_card,
  CASE WHEN
    is_member('support') THEN expiry_date
    ELSE regexp_replace(expiry_date, '^(0[1-9]|1[0-2])', 'XX')
  END AS expiry_date,
  CASE WHEN
    is_member('support') THEN email
    ELSE regexp_extract(email, '^.*@(.*)$', 1)
  END AS email,
  CASE WHEN
    is_member('support') THEN security_code
    ELSE 'XXX'
  END AS security_code
FROM {database_name}.customer_data
""")

# COMMAND ----------

display(sql(f"SHOW TABLES IN {database_name}"))

# COMMAND ----------

display(sql(f"SELECT * FROM {database_name}.customer_data_redacted"))
