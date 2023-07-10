# Databricks notebook source
# MAGIC %run "/utils/adls_utils/configuration"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("race_id", "1098")
race_id = dbutils.widgets.get("race_id")

# COMMAND ----------

constructors_schema = "constructor_id INT, constructor_ref STRING, name STRING, nationality STRING"

# COMMAND ----------

constructors_df = spark.read.option("header", True).schema(constructors_schema).csv(f"{raw_container_f1}{race_id}/constructors.csv")

# COMMAND ----------

constructors_final_df = constructors_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# DBTITLE 0,(
constructors_final_df.write.format("delta").mode("overwrite").saveAsTable("formula1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("success")