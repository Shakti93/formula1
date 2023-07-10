# Databricks notebook source
# MAGIC %md
# MAGIC race_results

# COMMAND ----------

dbutils.widgets.text("race_id", "1098")
race_id = dbutils.widgets.get("race_id")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS formula1_presentation.race_results
        (
            race_year INT,
            team_name STRING,
            driver_id INT,
            driver_name STRING,
            race_id INT,
            position INT,
            points INT,
            calculated_points INT,
            created_date TIMESTAMP,
            updated_date TIMESTAMP
        )
    USING DELTA
    PARTITIONED BY (race_id)      
""")

# COMMAND ----------

spark.sql(f"""
    SELECT races.year AS race_year,
            constructors.name AS team_name,
            drivers.driver_id AS driver_id,
            drivers.name AS driver_name,
            races.race_id AS race_id,
            results.position AS position,
            results.points AS points,
            CASE
                WHEN results.position < 11 THEN 11-results.position
                ELSE 0
                END AS calculated_points
            FROM formula1_processed.results AS results
                JOIN formula1_processed.races AS races
                    ON results.race_id = races.race_id
                JOIN formula1_processed.drivers AS drivers
                    ON results.driver_id = drivers.driver_id
                JOIN formula1_processed.constructors AS constructors
                    ON results.constructor_id = constructors.constructor_id
            WHERE results.race_id = {race_id}
""").createOrReplaceTempView("race_results_updates")

# COMMAND ----------

# DBTITLE 0,gt
spark.sql("""
    MERGE INTO
    formula1_presentation.race_results  tgt
    USING race_results_updates updates
    ON tgt.race_id = updates.race_id  AND tgt.driver_id = updates.driver_id
    WHEN MATCHED THEN
        UPDATE SET tgt.position = updates.position,
            tgt.points = updates.points,
            tgt.calculated_points = updates.calculated_points,
            tgt.updated_date = current_timestamp
    WHEN NOT MATCHED THEN
        INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points,created_date)
        VALUES (updates.race_year, updates.team_name, updates.driver_id, updates.driver_name, updates.race_id, updates.position, updates.points, updates.calculated_points, current_timestamp)
""")

# COMMAND ----------

dbutils.notebook.exit("success")