# Databricks notebook source
# MAGIC %md
# MAGIC ##step 1: Bronze Layer - Auto Loader for Raw Data Ingestion

# COMMAND ----------

df_joined = spark.read.parquet('/mnt/dev/sakila_dev_silver/city_country_joined')

# COMMAND ----------

# Auto Loader for Address Table
address_bronze_df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/mnt/dev/sakila_dev_bronze/address_schema") \
    .option("cloudFiles.useNotifications", "false") \
    .load("/mnt/dev/sakila_dev_external_tables/address")

# Auto Loader for City Table
city_bronze_df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/mnt/dev/sakila_dev_bronze/city_schema") \
    .option("cloudFiles.useNotifications", "false") \
    .load("/mnt/dev/sakila_dev_external_tables/city")

# Auto Loader for Country Table
country_bronze_df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/mnt/dev/sakila_dev_bronze/country_schema") \
    .option("cloudFiles.useNotifications", "false") \
    .load("/mnt/dev/sakila_dev_external_tables/country")

# Write data to Bronze layer (using overwrite mode or append based on the use case)
address_bronze_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/mnt/dev/sakila_dev_bronze/address_checkpoint") \
    .outputMode("append") \
    .start("/mnt/dev/sakila_dev_bronze/address")

city_bronze_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/mnt/dev/sakila_dev_bronze/city_checkpoint") \
    .outputMode("append") \
    .start("/mnt/dev/sakila_dev_bronze/city")

country_bronze_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/mnt/dev/sakila_dev_bronze/country_checkpoint") \
    .outputMode("append") \
    .start("/mnt/dev/sakila_dev_bronze/country")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Step 2: Silver Layer - Cleaning, Joining, and Writing to Silver

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

# Define schema for address, city, and country data
address_schema = StructType([
    StructField("address_id", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("address2", StringType(), True),
    StructField("district", StringType(), True),
    StructField("city_id", IntegerType(), True),
    StructField("postal_code", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("location", StringType(), True),
    StructField("last_update", TimestampType(), True),
    StructField("year", TimestampType(), True),
    StructField("month", TimestampType(), True),
    StructField("day", TimestampType(), True),
    StructField("rescued_data", TimestampType(), True)
])



city_schema = StructType([
    StructField("city_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("country_id", IntegerType(), True),
    StructField("last_update", TimestampType(), True),
    StructField("year", TimestampType(), True),
    StructField("month", TimestampType(), True),
    StructField("day", TimestampType(), True),
    StructField("rescued_data", TimestampType(), True)
])

country_schema = StructType([
    StructField("country_id", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("last_update", TimestampType(), True),
    StructField("year", TimestampType(), True),
    StructField("month", TimestampType(), True),
    StructField("day", TimestampType(), True),
    StructField("rescued_data", TimestampType(), True)
])

# Read data from Bronze Layer (streaming) with schema
address_bronze_df = spark.readStream.format("parquet").schema(address_schema).load("/mnt/dev/sakila_dev_bronze/address")
city_bronze_df = spark.readStream.format("parquet").schema(city_schema).load("/mnt/dev/sakila_dev_bronze/city")
country_bronze_df = spark.readStream.format("parquet").schema(country_schema).load("/mnt/dev/sakila_dev_bronze/country")

# Join city and country data to clean and standardize
city_country_silver_df = city_bronze_df.join(
    country_bronze_df,
    city_bronze_df["country_id"] == country_bronze_df["country_id"],
    "inner"
).select(
    city_bronze_df["city_id"],
    city_bronze_df["city"],
    country_bronze_df["country"],
)

# Write cleaned and joined data to the Silver layer
city_country_silver_df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "/mnt/dev/sakila_dev_silver/city_country_checkpoint") \
    .outputMode("append") \
    .start("/mnt/dev/sakila_dev_silver/city_country_joined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Gold Layer - Aggregation and Final Analytics

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col

# Read reference data (city-country) as a static DataFrame
city_country_schema = StructType([
    StructField("city_id", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
])
city_country_silver_df = spark.read.format("parquet").schema(city_country_schema).load("/mnt/dev/sakila_dev_silver/city_country_joined")

# Read streaming address data
address_bronze_df = spark.read.format("parquet").schema(address_schema).load("/mnt/dev/sakila_dev_bronze/address")

# Add watermark to event time column
address_bronze_df = address_bronze_df.withWatermark("last_update", "1 hour")

# Join streaming address with static city-country reference
address_enriched_df = address_bronze_df.join(
    city_country_silver_df,
    address_bronze_df["city_id"] == city_country_silver_df["city_id"],
    "inner"
)

# Perform aggregation
address_summary_df = address_enriched_df.groupBy("country").count().withColumnRenamed("count", "address_count")

# Write result to sink
address_summary_df.write \
    .format("parquet") \
    .option("checkpointLocation", "/mnt/dev/sakila_dev_gold/address_summary_checkpoint") \
    .mode("overwrite") \
    .save("/mnt/dev/sakila_dev_gold/address_summary")
