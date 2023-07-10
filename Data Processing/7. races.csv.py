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

race_schema = "race_id INT, year INT, round INT, circuit_id INT, name STRING, date DATE, time STRING, url STRING, fp1_date DATE, fp1_time STRING, fp2_date DATE, fp2_time STRING, fp3_date DATE, fp3_time STRING, quali_date DATE, quali_time STRING, sprint_date DATE, sprint_time STRING"

# COMMAND ----------

race_df = spark.read.option("header", True).schema(race_schema).csv(f"{raw_container_f1}{race_id}/races.csv")

# COMMAND ----------

race_final_df = race_df.drop(col("url")) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

deltatable_races = DeltaTable.forPath(spark, f"{processed_container_f1}races")

deltatable_races.alias("tgt") \
    .merge(race_final_df.alias("update"), "tgt.race_id = update.race_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# COMMAND ----------

dbutils.notebook.exit("success")