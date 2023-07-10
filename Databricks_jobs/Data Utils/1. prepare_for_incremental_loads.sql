-- Databricks notebook source
-- MAGIC %fs ls "/mnt/az1datalake/f1adls"

-- COMMAND ----------

DROP DATABASE IF EXISTS formula1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula1_processed
LOCATION "/mnt/az1datalake/f1adls/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS formula1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS formula1_presentation
LOCATION "/mnt/az1datalake/f1adls/presentation"