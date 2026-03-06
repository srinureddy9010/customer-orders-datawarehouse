import pandas as pd
import json
from datetime import datetime
import os

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
