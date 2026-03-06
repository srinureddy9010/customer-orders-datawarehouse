<<<<<<< HEAD

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

=======
import pandas as pd
import json
from datetime import datetime
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.data_quality import validate_all

print("Starting Data Warehouse Pipeline (Pandas Version)")
print("=" * 50)

# Create warehouse directory
os.makedirs("./warehouse", exist_ok=True)

# Read JSON data files
def read_json(path):
    with open(path, 'r') as f:
        data = json.load(f)
    return pd.DataFrame(data)

# Load raw data
customers = read_json("data/customers.json")
products = read_json("data/products.json")
payments = read_json("data/payments.json")
orders = read_json("data/orders.json")

print("\nCustomers Data")
print(customers.to_string(index=False))

print("\n\nProducts Data")
print(products.to_string(index=False))

print("\n\nPayments Data")
print(payments.to_string(index=False))

print("\n\nOrders Data")
print(orders.to_string(index=False))

# ===== DATA QUALITY VALIDATION =====
data_sources = {
    'customers': customers,
    'products': products,
    'orders': orders,
    'payments': payments
}

if not validate_all(data_sources):
    print("❌ Data quality validation failed. Aborting pipeline.")
    sys.exit(1)

# SCD2 for customers - Keep history with effective dates
def apply_scd2(df, target_path):
    print(f"\n\nApplying SCD2 for customer dimension → {target_path}")
    
    # Add temporal columns
    df_scd2 = df.copy()
    df_scd2['start_date'] = datetime.now()
    df_scd2['end_date'] = None
    df_scd2['is_current'] = True
    
    csv_path = target_path + ".csv"
    if os.path.exists(csv_path):
        print(f"  Loading existing records...")
        existing = pd.read_csv(csv_path)
        # Mark existing as inactive
        existing['is_current'] = False
        # Append new records
        result = pd.concat([existing, df_scd2], ignore_index=True)
    else:
        result = df_scd2
    
    result.to_csv(csv_path, index=False)
    print(f"  Created SCD2 table: {csv_path}")
    return result

# SCD1 for products - Overwrite with latest
def apply_scd1(df, target_path):
    print(f"\nApplying SCD1 for product dimension → {target_path}")
    
    csv_path = target_path + ".csv"
    df.to_csv(csv_path, index=False)
    print(f"  Created SCD1 table: {csv_path}")
    return df

# SCD3 for payments - Track current and previous
def apply_scd3(df, target_path):
    print(f"\nApplying SCD3 for payment dimension → {target_path}")
    
    df_scd3 = df.copy()
    df_scd3['current_method'] = df_scd3['method']
    df_scd3['previous_method'] = df_scd3['method']
    df_scd3 = df_scd3.drop(columns=['method'])
    
    csv_path = target_path + ".csv"
    df_scd3.to_csv(csv_path, index=False)
    print(f"  Created SCD3 table: {csv_path}")
    
    return df_scd3

# Create fact table
def create_fact_orders(df):
    print(f"\nCreating fact orders table")
    
    return df[['order_id', 'customer_id', 'product_id', 'payment_id', 'amount']]

# Apply transformations
print("\n" + "=" * 50)
print("APPLYING TRANSFORMATIONS")
print("=" * 50)

customer_dim = apply_scd2(customers, "./warehouse/dim_customer")
product_dim = apply_scd1(products, "./warehouse/dim_product")
payment_dim = apply_scd3(payments, "./warehouse/dim_payments")
fact_orders_df = create_fact_orders(orders)

# Write fact table
fact_csv = "./warehouse/fact_orders.csv"
fact_orders_df.to_csv(fact_csv, index=False)
print(f"\nCreated fact table: {fact_csv}")

print("\n" + "=" * 50)
print("✓ Warehouse tables created successfully!")
print("=" * 50)

print("\n\nWarehouse Structure:")
print("-" * 50)
for file in os.listdir('./warehouse'):
    path = os.path.join('./warehouse', file)
    if os.path.isfile(path):
        size = os.path.getsize(path)
        print(f"  {file:<30} ({size} bytes)")
>>>>>>> 306f2ed093b79972912f5c33314b1c1007fe220b
