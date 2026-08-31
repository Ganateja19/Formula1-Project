# Databricks notebook source
# MAGIC %md
# MAGIC # Transform sprints Data
# MAGIC
# MAGIC 1. Read bronze sprints table
# MAGIC 2. Keep only the columns required for analytics (DROP url)
# MAGIC 3. Standardise column names using snake case ( driverId -> driver_id, constructorID -> constructor_id, positionText -> finish_position_text)
# MAGIC 4. Rename columns to make them more meaningful(date->race_date, grid->grid_position, laps->completed_laps, number-> car_number, postion->final_position)
# MAGIC 5. Filter out rows where season, round, constructor_id or driver_id is null
# MAGIC 6. Remove Duplicate Records
# MAGIC 7. Transform values of the nationality to Title Case
# MAGIC 8. Write the transformed data to the silver sprints table 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read bronze sprints table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read bronze sprints table

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 to 4 Read source data, select required columns & standardise column names
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 8
sprints_df = (
    spark.table(bronze_table)
    .select(
        "date",
        "raceName",
        "round",
        "season",
        "constructorId",
        "driverId",
        "grid",
        "laps",
        "number",
        "points",
        "position",
        "positionText",
        "status",
        "ingestion_date",
        "source_file")
    .withColumnsRenamed({
        "driverId": "driver_id",
        "constructorId": "constructor_id",
        "racename": "race_name",
        "positionText": "finish_position_text",
        "date": "race_date",
        "grid": "grid_position",
        "laps": "completed_laps",
        "number": "car_number",
        "position": "final_position"
    })
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 & 6 Data Quality Checks

# COMMAND ----------

# DBTITLE 1,Cell 17
sprints_valid_df = (
    sprints_df
    .filter(
        F.col("season").isNotNull()
        & F.col("round").isNotNull()
        & F.col("constructor_id").isNotNull()
        & F.col("driver_id").isNotNull()
    )
    .dropDuplicates(["season","round","constructor_id","driver_id"])
)


# COMMAND ----------

display(sprints_df.count()- sprints_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform values of columns nationality to Title Case

# COMMAND ----------

sprints_final_df = (
    sprints_valid_df
    .withColumn('race_name', F.initcap(F.col('race_name'))))


# COMMAND ----------

display(sprints_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver sprints table 

# COMMAND ----------

(
    sprints_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))