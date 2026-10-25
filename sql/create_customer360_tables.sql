CREATE DATABASE IF NOT EXISTS ecommerce_customer360;
USE ecommerce_customer360;

-- 1. External staging table: orders
CREATE EXTERNAL TABLE IF NOT EXISTS stg_orders (
    order_id STRING,
    customer_id STRING,
    order_status STRING,
    order_purchase_timestamp STRING,
    order_approved_at STRING,
    order_delivered_carrier_date STRING,
    order_delivered_customer_date STRING,
    order_estimated_delivery_date STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/ecommerce_customer360/processed/orders_clean'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 2. External staging table: customers
CREATE EXTERNAL TABLE IF NOT EXISTS stg_customers (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix INT,
    customer_city STRING,
    customer_state STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/ecommerce_customer360/processed/customers_clean'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 3. External staging table: products
CREATE EXTERNAL TABLE IF NOT EXISTS stg_products (
    product_id STRING,
    product_category_name STRING,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g DOUBLE,
    product_length_cm DOUBLE,
    product_height_cm DOUBLE,
    product_width_cm DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE
LOCATION '/ecommerce_customer360/processed/products_clean'
TBLPROPERTIES ("skip.header.line.count"="1");

-- 4. Customer dimension
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id STRING,
    customer_unique_id STRING,
    customer_zip_code_prefix INT,
    customer_city STRING,
    customer_state STRING
)
STORED AS ORC;

-- 5. Product dimension
CREATE TABLE IF NOT EXISTS dim_product (
    product_id STRING,
    product_category_name STRING,
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g DOUBLE,
    product_length_cm DOUBLE,
    product_height_cm DOUBLE,
    product_width_cm DOUBLE
)
STORED AS ORC;
