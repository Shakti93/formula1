# Databricks notebook source
# MAGIC %run "/utils/adls_utils/configuration"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC circuits.csv

# COMMAND ----------

circuits_schema = "circuit_id INT, circuit_ref STRING, name STRING, location STRING, country STRING, latitude FLOAT, longitude FLOAT, altitude INT"

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv("/mnt/az1datalake/f1adls/raw/history/circuits.csv")

# COMMAND ----------

circuits_final_df = circuits_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("formula1_processed.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC constructors.csv

# COMMAND ----------

constructors_schema = "constructor_id INT, constructor_ref STRING, name STRING, nationality STRING"

# COMMAND ----------

constructors_df = spark.read.option("header", True).schema(constructors_schema).csv("/mnt/az1datalake/f1adls/raw/history/constructors.csv")

# COMMAND ----------

constructors_final_df = constructors_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# DBTITLE 0,(
constructors_final_df.write.format("delta").mode("overwrite").saveAsTable("formula1_processed.constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC drivers.csv

# COMMAND ----------

drivers_schema = "driver_id INT, driver_ref STRING, number INT, code STRING, forename STRING, surname STRING, dob DATE, nationality STRING"

# COMMAND ----------

drivers_df = spark.read.option("header", True).schema(drivers_schema).csv("/mnt/az1datalake/f1adls/raw/history/drivers.csv")

# COMMAND ----------

drivers_final_df = drivers_df.withColumn("name", concat("forename", lit(" "), "surname")).drop("forename").drop("surname") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

drivers_final_df.write.format("delta").mode("overwrite").saveAsTable("formula1_processed.drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC lap_times.json

# COMMAND ----------

lap_times_df = spark.read.json("/mnt/az1datalake/f1adls/raw/history/lap_times.json")

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumn("ingestion_date", current_timestamp()) \
    .select(col("driverId").cast("int").alias("driver_id"), col("lap").cast("int"), col("milliseconds").cast("int"), col("position").cast("int"), col("raceId").cast("int").alias("race_id"), col("time"), col("ingestion_date"))

# COMMAND ----------

lap_times_final_df.write.format("delta").mode("overwrite").partitionBy("race_id").saveAsTable("formula1_processed.lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC pit_stops.csv

# COMMAND ----------

pit_stops_schema = "race_id INT, driver_id INT, stop INT, lap INT, time STRING, duration STRING, milliseconds INT"

# COMMAND ----------

pit_stops_df = spark.read.option("header", True).schema(pit_stops_schema).csv("/mnt/az1datalake/f1adls/raw/history/pit_stops.csv")

# COMMAND ----------

pit_stops_df.write.format("delta").mode("overwrite").partitionBy("race_id").saveAsTable("formula1_processed.pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC qualifying.json

# COMMAND ----------

qualifying_df = spark.read.json("/mnt/az1datalake/f1adls/raw/history/qualifying.json")

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumn("ingestion_date", current_timestamp()) \
    .select(col("qualifyId").cast("int").alias("qualify_id"), col("raceId").cast("int").alias("race_id"), col("driverId").cast("int").alias("driver_id"), col("constructorId").cast("int").alias("constructor_id"), col("number").cast("int"), col("position").cast("int"), col("q1"), col("q2"), col("q3"), col("ingestion_date"))

# COMMAND ----------

# DBTITLE 0,a
qualifying_final_df.write.format("delta").mode("overwrite").partitionBy("race_id").saveAsTable("formula1_processed.qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC reaces.csv

# COMMAND ----------

race_schema = "race_id INT, year INT, round INT, circuit_id INT, name STRING, date DATE, time STRING, url STRING, fp1_date DATE, fp1_time STRING, fp2_date DATE, fp2_time STRING, fp3_date DATE, fp3_time STRING, quali_date DATE, quali_time STRING, sprint_date DATE, sprint_time STRING"

# COMMAND ----------

race_df = spark.read.option("header", True).schema(race_schema).csv("/mnt/az1datalake/f1adls/raw/history/races.csv")

# COMMAND ----------

race_final_df = race_df.drop(col("url")) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

race_final_df.write.format("delta").mode("overwrite").partitionBy("race_id").saveAsTable("formula1_processed.races")

# COMMAND ----------

# MAGIC %md
# MAGIC results.csv

# COMMAND ----------

results_schema = "result_id INT, race_id INT, driver_id INT, constructor_id INT, number INT, grid INT, position INT, position_text STRING, position_order INT, points FLOAT, laps INT, time STRING, milliseconds INT, fastest_lap INT, rank INT, fastest_lap_time STRING, fastest_lap_speed FLOAT, status_id INT"

# COMMAND ----------

results_df = spark.read.option("header", True).schema(results_schema).csv("/mnt/az1datalake/f1adls/raw/history/results.csv")

# COMMAND ----------

results_final_df = results_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_final_df.write.format("delta").mode("overwrite").partitionBy("race_id").saveAsTable("formula1_processed.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC formula1_processed.results