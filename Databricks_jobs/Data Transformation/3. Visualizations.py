# Databricks notebook source
dbutils.widgets.text("race_year", "2023")
race_year = dbutils.widgets.get("race_year")

# COMMAND ----------

display(spark.sql(f"""
SELECT * 
FROM formula1_presentation.driver_standings
WHERE race_year = {race_year}
ORDER BY rank
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT * 
FROM formula1_presentation.constructor_standings
WHERE race_year = {race_year}
ORDER BY rank
"""))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_name,
# MAGIC   sum(calculated_points) AS total_points, 
# MAGIC   count(1) AS total_races,
# MAGIC   round(sum(calculated_points)/count(1),2) AS avg_points
# MAGIC FROM formula1_presentation.race_results
# MAGIC WHERE driver_name IN (SELECT driver_name FROM formula1_presentation.driver_standings WHERE rank BETWEEN 1 AND 3)
# MAGIC GROUP BY driver_name
# MAGIC HAVING total_races > 100
# MAGIC ORDER BY avg_points DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT team_name,
# MAGIC   count(1) AS total_races,
# MAGIC   sum(calculated_points) AS total_points
# MAGIC FROM formula1_presentation.race_results
# MAGIC WHERE team_name IN (SELECT team_name FROM formula1_presentation.constructor_standings WHERE rank BETWEEN 1 AND 5)
# MAGIC GROUP BY team_name
# MAGIC HAVING total_races > 200
# MAGIC ORDER BY total_points DESC