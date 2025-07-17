# Databricks notebook source
# df1 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/nikhil/site.json",multiline=True)
# df2 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/nikhil/orders.json",multiline=True)
# df3 = spark.read.format("json").load("dbfs:/FileStore/shared_uploads/nikhil/products.json",multiline=True)

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/nikhil/order/order.csv")
df2 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/nikhil/site/site.csv")
df3 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/nikhil/product/product.csv")

# COMMAND ----------

df1.show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType,FloatType
import dlt,os
from pyspark.sql.functions import sum as _sum, col
# env = spark.conf.get("env")

site_schema = StructType([
    StructField("site_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("last_inventory_check", StringType(), True),
    StructField("status", StringType(), True)
])

@dlt.table(
    comment="Bronze table ingesting site data from JSON file in landing location",
    spark_conf={"pipelines.trigger.interval" : "10 seconds"},
    table_properties={"pipelines.autoOptimize.managed": "true"}
)
def incremental_site_bronze():
    return (
        spark.readStream.format("csv")
        .option("header", "true")  # First row contains column names
        .schema(site_schema)  # Enforce schema (mandatory)
        .load(f"dbfs:/FileStore/shared_uploads/nikhil/site/",multiline=True)
    )

# COMMAND ----------

product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", FloatType(), True),
    StructField("available", StringType(), True),
    StructField("color", StringType(), True),
])

@dlt.table(
    comment="Bronze table ingesting product data from JSON file in landing location",
    spark_conf={"pipelines.trigger.interval" : "10 seconds"},
    table_properties={"pipelines.autoOptimize.managed": "true"}
)
def incremental_product_bronze():
    return (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(product_schema)  # Enforce schema (mandatory)
        .load(f"dbfs:/FileStore/shared_uploads/nikhil/product/",multiline=True)
    )

# COMMAND ----------

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", FloatType(), True)
])

@dlt.table(
    comment="Bronze table ingesting order data from JSON file in landing location",
    spark_conf={"pipelines.trigger.interval" : "10 seconds"},
    table_properties={"pipelines.autoOptimize.managed": "true"}
)
def incremental_order_bronze():
    return (
        spark.readStream.format("csv")
        .option("header", "true")
        .schema(order_schema)  # Enforce schema (mandatory)
        .load(f"dbfs:/FileStore/shared_uploads/nikhil/order/",multiline=True)
    )

# COMMAND ----------

df2.show()

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp,current_date,countDistinct, max, length, to_date, coalesce, lit, lower

# SILVER
@dlt.table(
    comment="Silver table cleaning site data"
)
@dlt.expect("valid_date", "year(last_inventory_check) >= 2020")
def incremental_site_silver():
    return (
        dlt.read("incremental_site_bronze")
        .withColumn("last_inventory_check", to_date("last_inventory_check", "yyyy-MM-dd")) #Convert to date
        .withColumn("status", lower(col("status")))
        .withColumn("inserted_at", current_timestamp())  # Add inserted timestamp
        .dropDuplicates()
    )

@dlt.table(
    comment="Silver table cleaning product data"
)
@dlt.expect("non_negative_price", "price >= 0")
def incremental_product_silver():
    return (
        dlt.read("incremental_product_bronze")
        .filter(col("product_id").isNotNull())
        .withColumn("inserted_at", current_timestamp())  # Add inserted timestamp
        .dropDuplicates()
    )

@dlt.table(
    comment="Silver table cleaning order data"
)
@dlt.expect("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect("non_null_product_id", "product_id IS NOT NULL")
def incremental_order_silver():
    return (
        dlt.read("incremental_order_bronze")
        .filter(col("order_id").isNotNull())
        .withColumn("inserted_at", current_timestamp())  # Add inserted timestamp
        .dropDuplicates()
    )

# COMMAND ----------

@dlt.table(
    comment="Gold table aggregating total ordered quantity per product"
)
def gold_inventory():
    site = dlt.read("incremental_site_silver").select("site_id", "product_id")
    product = dlt.read("incremental_product_silver").select("product_id")
    order = dlt.read("incremental_order_silver").select("product_id", "order_id", "quantity")

    return (
        product
        .join(site, "product_id", "left")  
        .join(order, "product_id", "left")  
        .groupBy("product_id")
        .agg(
            _sum("quantity").cast("int").alias("total_quantity") 
        )
        .orderBy(col("product_id").asc()) 
    )