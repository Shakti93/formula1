# Databricks notebook source
# MAGIC %run "/utils/adls_utils/configuration"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("race_id", "1098")
race_id = dbutils.widgets.get("race_id")

# COMMAND ----------

drivers_schema = "driver_id INT, driver_ref STRING, number INT, code STRING, forename STRING, surname STRING, dob DATE, nationality STRING"

# COMMAND ----------

drivers_df = spark.read.option("header", True).schema(drivers_schema).csv(f"{raw_container_f1}{race_id}/drivers.csv")

# COMMAND ----------

drivers_final_df = drivers_df.withColumn("name", concat("forename", lit(" "), "surname")).drop("forename").drop("surname") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

drivers_final_df.write.format("delta").mode("overwrite").saveAsTable("formula1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("success")