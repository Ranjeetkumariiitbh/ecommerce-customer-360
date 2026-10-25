USE ecommerce_customer360;

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=2000;
SET hive.exec.max.dynamic.partitions.pernode=2000;
SET hive.exec.max.created.files=10000;

INSERT OVERWRITE TABLE fact_orders PARTITION (order_date)
SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP),
    CASE
        WHEN order_approved_at IS NULL OR trim(order_approved_at) = ''
        THEN NULL ELSE CAST(order_approved_at AS TIMESTAMP)
    END,
    CASE
        WHEN order_delivered_carrier_date IS NULL OR trim(order_delivered_carrier_date) = ''
        THEN NULL ELSE CAST(order_delivered_carrier_date AS TIMESTAMP)
    END,
    CASE
        WHEN order_delivered_customer_date IS NULL OR trim(order_delivered_customer_date) = ''
        THEN NULL ELSE CAST(order_delivered_customer_date AS TIMESTAMP)
    END,
    CASE
        WHEN order_estimated_delivery_date IS NULL OR trim(order_estimated_delivery_date) = ''
        THEN NULL ELSE CAST(order_estimated_delivery_date AS TIMESTAMP)
    END,
    substr(order_purchase_timestamp, 1, 10)
FROM stg_orders
WHERE order_purchase_timestamp IS NOT NULL
  AND length(order_purchase_timestamp) >= 10;
