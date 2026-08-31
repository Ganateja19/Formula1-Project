# Databricks notebook source
# helper function to add metadata for ingestion (source file name and ingestion date)

from pyspark.sql import functions as F

def add_ingestion_metadata(df):
    return  (
        df.withColumn('ingestion_date', F.current_timestamp())
            .withColumn('source_file', F.col('_metadata.file_path'))
    )

# COMMAND ----------

