-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Set-Up the Project environment for Formula1
-- MAGIC
-- MAGIC 1. Create External Location databricks-course-extdl1-formula1
-- MAGIC 2. Create catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Access Cloud Storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1@databrickscoursextdl1.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Create External location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databrickscoursextdl1_formula1
    URL 'abfss://formula1@databrickscoursextdl1.dfs.core.windows.net/'
    WITH (STORAGE CREDENTIAL `databricks-sc`)
    COMMENT 'External location for the demo container';

-- COMMAND ----------

ShOW CATALOGS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create catalog

-- COMMAND ----------

 CREATE CATALOG IF NOT EXISTS formula1
   MANAGED LOCATION 'abfss://formula1@databrickscoursextdl1.dfs.core.windows.net/'
    COMMENT 'This is the main catalog for the formula1 Project';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Schemas landing,bronze,silver,gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1.landing;
CREATE SCHEMA IF NOT EXISTS formula1.bronze
    MANAGED LOCATION 'abfss://formula1@databrickscoursextdl1.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1.silver
    MANAGED LOCATION 'abfss://formula1@databrickscoursextdl1.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1.gold
    MANAGED LOCATION 'abfss://formula1@databrickscoursextdl1.dfs.core.windows.net/gold';

-- COMMAND ----------

USE CATALOG formula1

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Volume Files

-- COMMAND ----------

-- DBTITLE 1,Cell 14
CREATE EXTERNAL VOLUME IF NOT EXISTS formula1.landing.files
LOCATION 'abfss://formula1@databrickscoursextdl1.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1/landing/files
-- MAGIC