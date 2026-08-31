# Databricks notebook source
# MAGIC %md
# MAGIC # Transform constructors Data
# MAGIC
# MAGIC 1. Read bronze constructors table
# MAGIC 2. Keepo only the columns required for analytics (DROP url)
# MAGIC 3. Standardise column names using snake case ( constructorID-> constructor_id)
# MAGIC 4. Rename columns to make them more meaningful (name -> constructor_name)
# MAGIC 5. Remove Duplicate Records
# MAGIC 6. Transform values of columns nationality to Title Case
# MAGIC 7. Write the transformed data to the silver constructors table 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Read bronze constructors table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read bronze constructors table

# COMMAND ----------

#constructors_df=spark.read.option('versionAsOf' , 0).table(bronze_table)


# COMMAND ----------

constructors_df = spark.table(bronze_table)

# COMMAND ----------

display(constructors_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Keep only the columns required for analytics (DROP url)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

construtors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Standardise column namnes using snake case 
# MAGIC ###4. Rename coulumns to make more meaningful

# COMMAND ----------

# constructors_renamed_df = (
#     constructors_selected_df
#     .withColumnRenamed("circuitId", "circuit_id")
#     .withColumnRenamed("circuitName", "circuit_name")
#     .withColumnRenamed("lat", "latitude")
#     .withColumnRenamed("long", "longitude")
# )

# COMMAND ----------

constructors_renamed_df = (
    construtors_dropped_df
    .withColumnsRenamed({
        "constructorId": "constructor_id",
        "name": "constructor_name"
    })
)

# COMMAND ----------

display(constructors_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Remove Duplicate Records

# COMMAND ----------

constructors_distnict_df = constructors_renamed_df.dropDuplicates(["constructor_id"])
display(constructors_distnict_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 6. Transform values of columns race_name to Title Case

# COMMAND ----------

constructors_final_df = (
    constructors_distnict_df
    .withColumn('nationality', F.initcap(F.col('nationality'))))


# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7. Write the transformed data to silver constructors table 

# COMMAND ----------

(
    constructors_final_df
    .write
    .mode("overwrite")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))