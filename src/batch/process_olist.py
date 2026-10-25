
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType
)

HDFS = "hdfs://localhost:9000"
RAW = f"{HDFS}/ecommerce_customer360/raw"
import os

OUT = os.environ.get(
    "CUSTOMER360_BATCH_OUT",
    f"{HDFS}/ecommerce_customer360/processed",
).rstrip("/")

spark = (
    SparkSession.builder
    .appName("EcommerceCustomer360-Batch")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Explicit source schemas: Sqoop output has no header.
order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", StringType()),
    StructField("order_approved_at", StringType()),
    StructField("order_delivered_carrier_date", StringType()),
    StructField("order_delivered_customer_date", StringType()),
    StructField("order_estimated_delivery_date", StringType()),
])

customer_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_unique_id", StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType()),
])

product_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_category_name", StringType()),
    StructField("product_name_lenght", StringType()),
    StructField("product_description_lenght", StringType()),
    StructField("product_photos_qty", StringType()),
    StructField("product_weight_g", StringType()),
    StructField("product_length_cm", StringType()),
    StructField("product_height_cm", StringType()),
    StructField("product_width_cm", StringType()),
])

def read_raw(name, schema):
    return (
        spark.read
        .schema(schema)
        .option("mode", "PERMISSIVE")
        .csv(f"{RAW}/{name}_initial")
    )

orders = read_raw("orders", order_schema)
customers = read_raw("customers", customer_schema)
products = read_raw("products", product_schema)

# Normalize text fields and parse available timestamps.
orders = (
    orders
    .withColumn("order_status", F.lower(F.trim("order_status")))
    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_approved_at",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "order_delivered_carrier_date",
        F.to_timestamp("order_delivered_carrier_date")
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date")
    )
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
)

customers = (
    customers
    .withColumn("customer_city", F.lower(F.trim("customer_city")))
    .withColumn("customer_state", F.upper(F.trim("customer_state")))
    .filter(F.col("customer_id").isNotNull())
    .dropDuplicates(["customer_id"])
)

products = (
    products
    .withColumn("product_category_name",
                F.lower(F.trim("product_category_name")))
    .withColumn("product_name_lenght",
                F.col("product_name_lenght").cast("int"))
    .withColumn("product_description_lenght",
                F.col("product_description_lenght").cast("int"))
    .withColumn("product_photos_qty",
                F.col("product_photos_qty").cast("int"))
    .withColumn("product_weight_g",
                F.col("product_weight_g").cast("double"))
    .withColumn("product_length_cm",
                F.col("product_length_cm").cast("double"))
    .withColumn("product_height_cm",
                F.col("product_height_cm").cast("double"))
    .withColumn("product_width_cm",
                F.col("product_width_cm").cast("double"))
    .filter(F.col("product_id").isNotNull())
    .dropDuplicates(["product_id"])
)

# Customer/order join: preserve orders with missing customer matches.
customer_orders = orders.join(
    customers,
    on="customer_id",
    how="left"
)

# Save to separate paths; never overwrite raw imports.
outputs = [
    (orders, f"{OUT}/orders_clean"),
    (customers, f"{OUT}/customers_clean"),
    (products, f"{OUT}/products_clean"),
    (customer_orders, f"{OUT}/customer_orders"),
]

for df, path in outputs:
    (
        df.write
        .mode("errorifexists")
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(path)
    )
    print(f"Saved {path}: {df.count()} rows")

print("=== BATCH VALIDATION ===")
print(f"Orders: {orders.count()}")
print(f"Customers: {customers.count()}")
print(f"Products: {products.count()}")
print(f"Joined customer_orders: {customer_orders.count()}")

print("Missing purchase timestamps:",
      orders.filter(F.col("order_purchase_timestamp").isNull()).count())

print("Orders without customer match:",
      customer_orders.filter(F.col("customer_unique_id").isNull()).count())

spark.stop()
