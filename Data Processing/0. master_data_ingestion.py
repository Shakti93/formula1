# Databricks notebook source
dbutils.widgets.text("race_id", "1098")
race_id = dbutils.widgets.get("race_id")

# COMMAND ----------

notebook_list = [
    "1. circuits.csv",
    "2. constructors.csv",
    "3. drivers.csv",
    "4. lap_times.json",
    "5. pit_stops.csv",
    "6. qualifying.json",
    "7. races.csv",
    "8. results.csv"
]
print(notebook_list)

# COMMAND ----------

# DBTITLE 0,h 
for notebook in notebook_list:
    notebook_output = dbutils.notebook.run(notebook,0,{"race_id":race_id})
    print(f"Notebook '{notebook}' ran successfully with output: {notebook_output}")