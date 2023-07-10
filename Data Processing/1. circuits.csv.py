# Databricks notebook source
# MAGIC %run "/utils/adls_utils/configuration"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("race_id", "1098")
race_id = dbutils.widgets.get("race_id")

# COMMAND ----------

circuits_schema = "circuit_id INT, circuit_ref STRING, name STRING, location STRING, country STRING, latitude FLOAT, longitude FLOAT, altitude INT"

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv(f"{raw_container_f1}{race_id}/circuits.csv")

# COMMAND ----------

circuits_final_df = circuits_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("formula1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("success")