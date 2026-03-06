import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.extract.extract_data import create_spark, read_json
from domain.product_scd1 import apply_scd1
from domain.customer_scd2 import apply_scd2
from domain.payment_scd3 import apply_scd3
from domain.fact_orders import create_fact_orders

spark = create_spark()

print("Starting Data Warehouse Pipeline")

customers = read_json(spark, "data/customers.json")
products = read_json(spark, "data/products.json")
payments = read_json(spark, "data/payments.json")
orders = read_json(spark, "data/orders.json")

print("Customers Data")
customers.show()

print("Products Data")
products.show()

print("Payments Data")
payments.show()

print("Orders Data")
orders.show()

# Apply SCD transformations
customer_dim = apply_scd2(spark, customers, "./warehouse/dim_customer")
payment_dim = apply_scd3(payments)

# Write product dimension
apply_scd1(spark, products, "./warehouse/dim_product")

print("Warehouse tables created successfully")
# Payment dimension
payment_dim = apply_scd3(payments)

# Fact table
fact_orders = create_fact_orders(orders)

# Write fact table
fact_orders.coalesce(1).write.mode("overwrite").option("header", "true").csv("warehouse/fact_orders")