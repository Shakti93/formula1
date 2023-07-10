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

qualifying_df = spark.read.json(f"{raw_container_f1}{race_id}/qualifying.json")

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumn("ingestion_date", current_timestamp()) \
    .select(col("qualifyId").cast("int").alias("qualify_id"), col("raceId").cast("int").alias("race_id"), col("driverId").cast("int").alias("driver_id"), col("constructorId").cast("int").alias("constructor_id"), col("number").cast("int"), col("position").cast("int"), col("q1"), col("q2"), col("q3"), col("ingestion_date"))

# COMMAND ----------

deltatable_qualifying = DeltaTable.forPath(spark, f"{processed_container_f1}qualifying")

deltatable_qualifying.alias("tgt") \
    .merge(qualifying_final_df.alias("update"), "tgt.qualify_id = update.qualify_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

dbutils.notebook.exit("success")