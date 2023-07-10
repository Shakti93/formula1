# Databricks notebook source
# MAGIC %md
# MAGIC driver_standings

# COMMAND ----------

dbutils.widgets.text("race_year", "2023")
race_year = dbutils.widgets.get("race_year")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS formula1_presentation.driver_standings
          (
              race_year INT,
              driver_id INT,
              driver_name STRING,
              wins INT,
              total_points INT,
              rank INT
          )
        USING DELTA
        PARTITIONED BY (race_year)
""")

# COMMAND ----------

spark.sql(f"""
    SELECT race_year,
        driver_id, 
        driver_name, 
        SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS wins, 
        SUM(points) as total_points,
        RANK() OVER (PARTITION BY race_year ORDER BY SUM(points) DESC, SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) DESC) as rank
    FROM formula1_presentation.race_results
    WHERE race_year = {race_year}
    GROUP BY race_year, driver_id, driver_name
    ORDER BY total_points DESC       
""").createOrReplaceTempView("driver_standings_updates")

# COMMAND ----------

spark.sql("""
    MERGE INTO formula1_presentation.driver_standings tgt
    USING driver_standings_updates updates
    ON tgt.race_year = updates.race_year AND tgt.driver_id = updates.driver_id
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

# COMMAND ----------

# MAGIC %md
# MAGIC team_standings

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS formula1_presentation.constructor_standings
          (
              race_year INT,
              team_name STRING,
              wins INT,
              total_points INT,
              rank INT
          )
        USING DELTA
        PARTITIONED BY (race_year)
""")

# COMMAND ----------

spark.sql(f"""
    SELECT race_year,
        team_name,
        SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) AS wins,
        SUM(points) AS total_points,
        RANK() OVER(PARTITION BY race_year ORDER BY SUM(points) DESC, SUM(CASE WHEN position = 1 THEN 1 ELSE 0 END) DESC) as rank
        FROM formula1_presentation.race_results
        WHERE race_year = {race_year}
        GROUP BY race_year, team_name
""").createOrReplaceTempView("constructor_standings_updates")

# COMMAND ----------

spark.sql("""
    MERGE INTO formula1_presentation.constructor_standings tgt
    USING constructor_standings_updates updates
        ON tgt.race_year = updates.race_year AND tgt.team_name = updates.team_name
    WHEN MATCHED THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")

# COMMAND ----------

dbutils.notebook.exit("success")