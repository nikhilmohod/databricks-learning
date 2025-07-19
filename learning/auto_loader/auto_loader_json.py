# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

# COMMAND ----------

# Create Spark session
spark = SparkSession.builder \
    .appName("Auto Loader Process") \
    .getOrCreate()

# Input path configuration
input_path = "dbfs:/mnt/landing"

# Output path for processed data
output_path = "dbfs:/mnt/landing"

# COMMAND ----------

# Load the data using Auto Loader with directory listing
df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")  # Specify input format
      .option("cloudFiles.useNotifications", "false")  # Enable directory listing mode
      .load(input_path))

# COMMAND ----------

# Apply transformations
transformed_df = (df
    .withColumn("ingest_timestamp", current_timestamp())  # Add ingestion timestamp
    .withColumn("status", lit("processed"))  # Add a static column
)

# COMMAND ----------

# Write the transformed data to a Delta table 
transformed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .start(output_path)

print("Auto Loader pipeline started.")
