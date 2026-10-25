import os
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
RAW = os.path.join(ROOT, "data/raw")
OUTPUT = os.path.join(ROOT, "data/processed/rag")

os.makedirs(OUTPUT, exist_ok=True)

FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
}

def load_csv(name):
    path = os.path.join(RAW, FILES[name])
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    return pd.read_csv(path)

customers = load_csv("customers")
orders = load_csv("orders")
products = load_csv("products")

print("===== DATASET COUNTS =====")
print("Customers:", len(customers))
print("Orders:", len(orders))
print("Products:", len(products))

print("===== CREATING KNOWLEDGE DOCUMENTS =====")

documents = []

documents.append(
    "ECOMMERCE CUSTOMER 360 DATASET OVERVIEW\n"
    f"Customers dataset contains {len(customers)} rows.\n"
    f"Orders dataset contains {len(orders)} rows.\n"
    f"Products dataset contains {len(products)} rows.\n"
    "These datasets are available for customer and order analytics."
)

documents.append(
    "CUSTOMER DATASET INFORMATION\n"
    f"Columns: {', '.join(customers.columns.astype(str))}\n"
    f"Unique customer IDs: {customers['customer_id'].nunique()}\n"
    f"Unique customer identities: {customers['customer_unique_id'].nunique()}\n"
)

documents.append(
    "ORDER DATASET INFORMATION\n"
    f"Columns: {', '.join(orders.columns.astype(str))}\n"
    f"Unique orders: {orders['order_id'].nunique()}\n"
    f"Order statuses:\n{orders['order_status'].value_counts().to_string()}\n"
)

documents.append(
    "PRODUCT DATASET INFORMATION\n"
    f"Columns: {', '.join(products.columns.astype(str))}\n"
    f"Unique products: {products['product_id'].nunique()}\n"
)

if "product_category_name" in products.columns:
    categories = products["product_category_name"].fillna("unknown")
    documents.append(
        "PRODUCT CATEGORY INFORMATION\n"
        f"Category counts:\n{categories.value_counts().head(30).to_string()}\n"
    )

output_path = os.path.join(OUTPUT, "knowledge.txt")

with open(output_path, "w", encoding="utf-8") as f:
    f.write("\n\n".join(documents))

print("Knowledge document:", output_path)
print("Document sections:", len(documents))
print("KNOWLEDGE_PREPARATION=PASS")
