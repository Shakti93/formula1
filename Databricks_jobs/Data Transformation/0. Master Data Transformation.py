# Databricks notebook source
dbutils.widgets.text("race_id", "1098")
race_id = dbutils.widgets.get("race_id")

# COMMAND ----------

race_year = spark.sql(f"SELECT max(year) FROM formula1_processed.races WHERE race_id = {race_id}").collect()[0][0]

# COMMAND ----------

notebook_output = dbutils.notebook.run("1. race_results", 0, {"race_id": race_id})
print(notebook_output)

# COMMAND ----------

notebook_output = dbutils.notebook.run("2. driver and constructor standings", 0, {"race_year": race_year})
print(notebook_output)

# COMMAND ----------

notebook_output = dbutils.notebook.run("3. Visualizations", 0, {"race_year": race_year})
print(notebook_output)