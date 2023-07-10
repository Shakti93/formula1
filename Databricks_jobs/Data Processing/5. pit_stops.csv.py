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

pit_stops_schema = "race_id INT, driver_id INT, stop INT, lap INT, time STRING, duration STRING, milliseconds INT"

# COMMAND ----------

pit_stops_df = spark.read.option("header", True).schema(pit_stops_schema).csv(f"{raw_container_f1}{race_id}/pit_stops.csv")

# COMMAND ----------

deltatable_pit_stops = DeltaTable.forPath(spark, f"{processed_container_f1}pit_stops")

deltatable_pit_stops.alias("tgt") \
    .merge(pit_stops_df.alias("update"), "tgt.race_id = update.race_id AND tgt.driver_id = update.driver_id AND tgt.stop = update.stop") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

dbutils.notebook.exit("success")