
import pandas as pd
from datetime import datetime
import os
import requests

print("Starting Customer Orders Data Warehouse Pipeline")
print("=" * 60)

# ----------------------------------------------------
# Create warehouse directory
# ----------------------------------------------------
WAREHOUSE_PATH = "./warehouse"
os.makedirs(WAREHOUSE_PATH, exist_ok=True)

# ----------------------------------------------------
# API INGESTION FUNCTION
# ----------------------------------------------------
def fetch_api_data(url):

    response = requests.get(url)
    response.raise_for_status()

    return response.json()


# ----------------------------------------------------
# EXTRACT DATA (API ONLY)
# ----------------------------------------------------
print("\nExtracting Data from APIs...")

# Customers API
customers_api = "https://jsonplaceholder.typicode.com/users"
customers_data = fetch_api_data(customers_api)
customers = pd.DataFrame(customers_data)
print("Customers loaded from API")

# Products API
products_api = "https://dummyjson.com/products"
products_data = fetch_api_data(products_api)
products = pd.DataFrame(products_data["products"])
print("Products loaded from API")

import numpy as np

# Payments API
payments_api = "https://dummyjson.com/users"

payments_data = fetch_api_data(payments_api)

payments = pd.DataFrame(payments_data["users"])[["id"]]

payment_methods = ["UPI", "Credit Card", "Debit Card", "Net Banking"]

payments["method"] = np.random.choice(payment_methods, len(payments))

print("Payments loaded from API")

# Orders API
orders_api = "https://dummyjson.com/carts"
orders_data = fetch_api_data(orders_api)
orders = pd.DataFrame(orders_data["carts"])
print("Orders loaded from API")

# ----------------------------------------------------
# SHOW SAMPLE DATA
# ----------------------------------------------------
print("\nCustomers Sample")
print(customers.head())

print("\nProducts Sample")
print(products.head())

print("\nPayments Sample")
print(payments.head())

print("\nOrders Sample")
print(orders.head())

# ----------------------------------------------------
# DATA QUALITY CHECKS
# ----------------------------------------------------
def validate_dataset(df, name):

    if df.empty:
        raise ValueError(f"{name} dataset is empty")

    if df.isnull().sum().sum() > 0:
        print(f"Warning: {name} contains NULL values")

    print(f"{name} validation passed")


print("\nRunning Data Quality Checks")

validate_dataset(customers, "Customers")
validate_dataset(products, "Products")
validate_dataset(payments, "Payments")
validate_dataset(orders, "Orders")

# ----------------------------------------------------
# SCD2 CUSTOMER DIMENSION
# ----------------------------------------------------
def apply_scd2(df, path):

    print("\nApplying SCD2 - Customer Dimension")

    df_scd2 = df.copy()

    df_scd2["start_date"] = datetime.now()
    df_scd2["end_date"] = None
    df_scd2["is_current"] = True

    csv_path = f"{path}.csv"

    if os.path.exists(csv_path):

        existing = pd.read_csv(csv_path)
        existing["is_current"] = False

        df_scd2 = pd.concat([existing, df_scd2], ignore_index=True)

    df_scd2.to_csv(csv_path, index=False)

    print(f"Customer dimension created: {csv_path}")

    return df_scd2


# ----------------------------------------------------
# SCD1 PRODUCT DIMENSION
# ----------------------------------------------------
def apply_scd1(df, path):

    print("\nApplying SCD1 - Product Dimension")

    csv_path = f"{path}.csv"

    df.drop_duplicates(subset=["id"], keep="last").to_csv(csv_path, index=False)

    print(f"Product dimension created: {csv_path}")

    return df


# ----------------------------------------------------
# SCD3 PAYMENT DIMENSION
# ----------------------------------------------------
def apply_scd3(df, path):

    print("\nApplying SCD3 - Payment Dimension")

    df_scd3 = df.copy()

    if "method" in df_scd3.columns:
        df_scd3["previous_method"] = None
        df_scd3.rename(columns={"method": "current_method"}, inplace=True)

    csv_path = f"{path}.csv"

    df_scd3.to_csv(csv_path, index=False)

    print(f"Payment dimension created: {csv_path}")

    return df_scd3


# ----------------------------------------------------
# FACT TABLE
# ----------------------------------------------------
def create_fact_orders(df, path):

    print("\nCreating Incremental Fact Orders Table")

    fact_df = df[["id", "userId", "total"]]

    csv_path = f"{path}.csv"

    if os.path.exists(csv_path):

        existing = pd.read_csv(csv_path)

        # keep only new records
        fact_df = fact_df[~fact_df["id"].isin(existing["id"])]

        result = pd.concat([existing, fact_df], ignore_index=True)

    else:
        result = fact_df

    result.to_csv(csv_path, index=False)

    print(f"Fact table updated: {csv_path}")

    return result




# ----------------------------------------------------
# TRANSFORM
# ----------------------------------------------------
print("\nApplying Transformations")
print("-" * 60)

customer_dim = apply_scd2(customers, f"{WAREHOUSE_PATH}/dim_customer")

product_dim = apply_scd1(products, f"{WAREHOUSE_PATH}/dim_product")

payment_dim = apply_scd3(payments, f"{WAREHOUSE_PATH}/dim_payment")

fact_orders = create_fact_orders(
    orders,
    f"{WAREHOUSE_PATH}/fact_orders"
)

# ----------------------------------------------------
# FINAL OUTPUT
# ----------------------------------------------------
print("\nWarehouse Tables Created Successfully")
print("-" * 60)

for file in os.listdir(WAREHOUSE_PATH):
    path = os.path.join(WAREHOUSE_PATH, file)
    if os.path.isfile(path):
        size = os.path.getsize(path)
        print(f"{file:<25} {size} bytes")

print("\nPipeline Completed Successfully")

