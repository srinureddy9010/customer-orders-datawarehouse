
import pandas as pd
import requests

print("Starting API Data Extraction")
print("=" * 60)

# ---------------------------------------------------
# Generic API Fetch Function
# ---------------------------------------------------
def fetch_api_data(url):

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")

    data = response.json()

    return data


# ---------------------------------------------------
# Customers API
# ---------------------------------------------------
customers_api = "https://jsonplaceholder.typicode.com/users"

customers_data = fetch_api_data(customers_api)

customers_df = pd.DataFrame(customers_data)

print("\nCustomers Data (Sample)")
print(customers_df.head())


# ---------------------------------------------------
# Products API
# ---------------------------------------------------
products_api = "https://dummyjson.com/products"

products_data = fetch_api_data(products_api)

# dummyjson returns products inside a nested field
products_df = pd.DataFrame(products_data["products"])

print("\nProducts Data (Sample)")
print(products_df.head())


# ---------------------------------------------------
# Payments API (Mock API)
# ---------------------------------------------------
payments_api = "https://mocki.io/v1/ce5f60e2-0c5b-4db2-8e1e-3d6b2b9f1e7e"

payments_data = fetch_api_data(payments_api)

payments_df = pd.DataFrame(payments_data)

print("\nPayments Data (Sample)")
print(payments_df.head())


# ---------------------------------------------------
# Orders API (Simulated API)
# ---------------------------------------------------
orders_api = "https://dummyjson.com/carts"

orders_data = fetch_api_data(orders_api)

orders_df = pd.DataFrame(orders_data["carts"])

print("\nOrders Data (Sample)")
print(orders_df.head())


# ---------------------------------------------------
# Summary
# ---------------------------------------------------
print("\n" + "=" * 60)
print("Data Extraction Completed Successfully")
print("=" * 60)

print(f"Customers Records: {len(customers_df)}")
print(f"Products Records: {len(products_df)}")
print(f"Payments Records: {len(payments_df)}")
print(f"Orders Records: {len(orders_df)}")

