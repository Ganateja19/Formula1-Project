# Databricks notebook source
# MAGIC %md
# MAGIC # Transform races Data
# MAGIC
# MAGIC 1. Read bronze races table
# MAGIC 2. Keepo only the columns required for analytics (DROP url)
# MAGIC 3. Standardise column names using snake case ( raceName-> race_name, CircuitID-> circuit_id)
# MAGIC 4. Rename coulumns to make more meaningful
# MAGIC 5. Remove Duplicate Records
# MAGIC 6. Transform values of columns circuit_name and locality to Title Case
# MAGIC 7. Write the transformed data to the silver races table 
# MAGIC ![](![image_1788029436683.png](./image_1788029436683.png "image_1788029436683.png"))
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read bronze races table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - aread bronze races table

# COMMAND ----------

#races_df=spark.read.option('versionAsOf' , 0).table(bronze_table)


# COMMAND ----------

races_df = spark.table(bronze_table)

# COMMAND ----------

display(races_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analytics (DROP url)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_date"),
    F.col("source_file")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Standardise column namnes using snake case ( CircuitID-> circuit_id)
# MAGIC ###4. Rename coulumns to make more meaningful

# COMMAND ----------

# races_renamed_df = (
#     races_selected_df
#     .withColumnRenamed("circuitId", "circuit_id")
#     .withColumnRenamed("circuitName", "circuit_name")
#     .withColumnRenamed("lat", "latitude")
#     .withColumnRenamed("long", "longitude")
# )

# COMMAND ----------

races_renamed_df = (
    races_selected_df
    .withColumnsRenamed({
        "circuitId": "circuit_id",
        "raceName": "race_name",
        "date": "race_date",

    })
)

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove Duplicate Records

# COMMAND ----------

#races_distnict_df = races_valid_df.distinct()
#display(races_distnict_df)

# COMMAND ----------

races_distnict_df = races_renamed_df.dropDuplicates(["season", "round"])
display(races_distnict_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform values of columns race_name to Title Case

# COMMAND ----------

races_final_df = (
    races_distnict_df
    .withColumn('race_name', F.initcap(F.col('race_name')))
)

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver races table 

# COMMAND ----------

(
    races_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))