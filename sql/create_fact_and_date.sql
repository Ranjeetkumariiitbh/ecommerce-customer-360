USE ecommerce_customer360;

-- Date dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT,
    full_date DATE,
    year_num INT,
    month_num INT,
    day_num INT,
    day_of_week INT
)
STORED AS ORC;

-- Order-level fact table, partitioned by purchase date
CREATE TABLE IF NOT EXISTS fact_orders (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
)
PARTITIONED BY (order_date STRING)
STORED AS ORC;

-- Populate date dimension from actual order purchase dates
INSERT OVERWRITE TABLE dim_date
SELECT DISTINCT
    CAST(regexp_replace(substr(order_purchase_timestamp, 1, 10), '-', '') AS INT),
    CAST(substr(order_purchase_timestamp, 1, 10) AS DATE),
    year(CAST(substr(order_purchase_timestamp, 1, 10) AS DATE)),
    month(CAST(substr(order_purchase_timestamp, 1, 10) AS DATE)),
    day(CAST(substr(order_purchase_timestamp, 1, 10) AS DATE)),
    dayofweek(CAST(substr(order_purchase_timestamp, 1, 10) AS DATE))
FROM stg_orders
WHERE order_purchase_timestamp IS NOT NULL
  AND length(order_purchase_timestamp) >= 10;

-- Enable dynamic partitions for this load
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Populate fact table and derive partition from purchase timestamp
INSERT OVERWRITE TABLE fact_orders PARTITION (order_date)
SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP),
    CASE WHEN order_approved_at IS NULL OR trim(order_approved_at) = ''
         THEN NULL ELSE CAST(order_approved_at AS TIMESTAMP) END,
    CASE WHEN order_delivered_carrier_date IS NULL OR trim(order_delivered_carrier_date) = ''
         THEN NULL ELSE CAST(order_delivered_carrier_date AS TIMESTAMP) END,
    CASE WHEN order_delivered_customer_date IS NULL OR trim(order_delivered_customer_date) = ''
         THEN NULL ELSE CAST(order_delivered_customer_date AS TIMESTAMP) END,
    CASE WHEN order_estimated_delivery_date IS NULL OR trim(order_estimated_delivery_date) = ''
         THEN NULL ELSE CAST(order_estimated_delivery_date AS TIMESTAMP) END,
    substr(order_purchase_timestamp, 1, 10)
FROM stg_orders
WHERE order_purchase_timestamp IS NOT NULL
  AND length(order_purchase_timestamp) >= 10;
