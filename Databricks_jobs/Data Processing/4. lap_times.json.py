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

lap_times_df = spark.read.json(f"{raw_container_f1}{race_id}/lap_times.json")

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumn("ingestion_date", current_timestamp()) \
    .select(col("driverId").cast("int").alias("driver_id"), col("lap").cast("int"), col("milliseconds").cast("int"), col("position").cast("int"), col("raceId").cast("int").alias("race_id"), col("time"), col("ingestion_date"))

# COMMAND ----------

deltatable_lap_times = DeltaTable.forPath(spark, f"{processed_container_f1}lap_times")

deltatable_lap_times.alias("tgt") \
    .merge(lap_times_final_df.alias("update"), "tgt.race_id = update.race_id AND tgt.driver_id = update.race_id AND tgt.lap = update.lap") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

dbutils.notebook.exit("success")