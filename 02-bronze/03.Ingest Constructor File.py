# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest Constructer.json file
# MAGIC
# MAGIC 1. Read the file using spark dataframe reader API
# MAGIC 2. Add Metadata Columns
# MAGIC - Source File
# MAGIC - Ingestion Timestamp
# MAGIC 3. Write to bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step-1 Read the CSV file  using dataframe reader API

# COMMAND ----------

# Define the schema

constructors_schema = """constructorId STRING, 
                            name STRING, 
                            nationality STRING, 
                            url STRING
                        """


# COMMAND ----------

## Read data from constructor file

constructors_df = (
    spark.read
    .format('json')
    .schema(constructors_schema)
    .option('mode', 'FAILFAST')
    .load(source_file)
)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP2 - Add Metadata Columns

# COMMAND ----------


constructors_final_df= add_ingestion_metadata(constructors_df)

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 Write to bronze delta table

# COMMAND ----------

# DBTITLE 1,Cell 10
(
    constructors_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

spark.table('formula1.bronze.constructors')