# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits. csv file
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

source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step1 - Read the CSV file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema= StructType([
    StructField('circuitId', StringType(), True),
    StructField('url', StringType(), True),
    StructField('circuitName', StringType(), True),
    StructField('lat', DoubleType(), True),
    StructField('long', DoubleType(), True),
    StructField('locality', StringType(), True),
    StructField('country', StringType(), True)
])



# COMMAND ----------

circuits_df= (
    spark.read
    .format('csv')
    .option('header', True)
#  .option('inferSchema', True)
    .schema(circuits_schema)
    .load(source_file)
)


# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP2 - Add Metadata Columns

# COMMAND ----------

circuits_final_df= add_ingestion_metadata(circuits_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 Write to bronze delta table

# COMMAND ----------

# DBTITLE 1,Cell 10
(
    circuits_final_df
    .write
    .mode('overwrite')
    .option('overwriteSchema', True)
    .format('delta')
    .saveAsTable('formula1.bronze.circuits')
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM formula1.bronze.circuits

# COMMAND ----------

spark.table('formula1.bronze.circuits')