-- Databricks notebook source
CREATE OR REFRESH LIVE TABLE incremental_site_bronze
AS SELECT * FROM read_files(
  "dbfs:/FileStore/shared_uploads/nikhil/site/", --add your dbfs location path
  format => "csv"
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE incremental_product_bronze
AS SELECT * FROM read_files(
  "dbfs:/FileStore/shared_uploads/nikhil/product/",--add your dbfs location path
  format => "csv"
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE incremental_order_bronze
AS SELECT * FROM read_files(
  "dbfs:/FileStore/shared_uploads/nikhil/order/",--add your dbfs location path
  format => "csv"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Silver Layer

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE incremental_site_silver
(CONSTRAINT valid_date EXPECT (year(last_inventory_check) >= 2020) ON VIOLATION DROP ROW)
COMMENT "Silver table cleaning site data"
AS
SELECT DISTINCT
    site_id,
    product_id,
    TO_DATE(last_inventory_check, 'yyyy-MM-dd') AS last_inventory_check,
    LOWER(status) AS status,
    CURRENT_TIMESTAMP() AS inserted_at
FROM LIVE.incremental_site_bronze

-- COMMAND ----------



CREATE OR REFRESH LIVE TABLE incremental_product_silver
(CONSTRAINT non_negative_price EXPECT (price >= 0) ON VIOLATION DROP ROW)
COMMENT "Silver table cleaning product data"
AS
SELECT DISTINCT
    product_id,
    name,
    category,
    price,
    available,
    color,
    CURRENT_TIMESTAMP() AS inserted_at
FROM LIVE.incremental_product_bronze
WHERE product_id IS NOT NULL

-- COMMAND ----------


CREATE OR REFRESH LIVE TABLE incremental_order_silver(
    CONSTRAINT non_null_order_id EXPECT (order_id IS NOT NULL) ,
    CONSTRAINT non_null_product_id EXPECT (product_id IS NOT NULL)
)
COMMENT "Silver table cleaning order data"
AS
SELECT DISTINCT
    order_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    CURRENT_TIMESTAMP() AS inserted_at
FROM
    LIVE.incremental_order_bronze
WHERE
    order_id IS NOT NULL;


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Gold Layer

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_inventory
COMMENT "Gold table aggregating total ordered quantity per product"
AS
SELECT
    p.product_id,
    CAST(SUM(o.quantity) AS INT) AS total_quantity
FROM
    LIVE.incremental_product_silver p
LEFT JOIN LIVE.incremental_site_silver s
    ON p.product_id = s.product_id
LEFT JOIN LIVE.incremental_order_silver o
    ON p.product_id = o.product_id
GROUP BY
    p.product_id
ORDER BY
    p.product_id ASC