# Databricks notebook source
# MAGIC %run "/utils/adls_utils/configuration"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

dbutils.widgets.text("race_id", "1098")
race_id = dbutils.widgets.get("race_id")

# COMMAND ----------

results_schema = "result_id INT, race_id INT, driver_id INT, constructor_id INT, number INT, grid INT, position INT, position_text STRING, position_order INT, points FLOAT, laps INT, time STRING, milliseconds INT, fastest_lap INT, rank INT, fastest_lap_time STRING, fastest_lap_speed FLOAT, status_id INT"

# COMMAND ----------

results_df = spark.read.option("header", True).schema(results_schema).csv(f"{raw_container_f1}{race_id}/results.csv")

# COMMAND ----------

results_final_df = results_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

deltatable_results = DeltaTable.forPath(spark, f"{processed_container_f1}results")

deltatable_results.alias("tgt") \
    .merge(results_final_df.alias("update"), "tgt.result_id = update.result_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

dbutils.notebook.exit("success")