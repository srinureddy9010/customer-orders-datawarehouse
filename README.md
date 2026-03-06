
# Customer Orders Data Warehouse

## Project Overview

This project builds a **Mini Data Warehouse Pipeline** that processes customer orders data using **batch processing** and implements **Slowly Changing Dimensions (SCD1, SCD2, SCD3)**.

The pipeline ingests data from **multiple APIs**, performs **data quality checks**, applies **dimension transformations**, and generates **fact and dimension tables** for analytics.

The project simulates a **real-world data engineering workflow** including:

* API Data Ingestion
* Data Quality Validation
* Slowly Changing Dimensions
* Incremental Batch Processing
* Data Warehouse Modeling


---

# Architecture

```
                +---------------------+
                |    External APIs    |
                |---------------------|
                | Customers API       |
                | Products API        |
                | Payments API        |
                | Orders API          |
                +----------+----------+
                           |
                           v
                    Data Extraction
                           |
                           v
                    Data Quality Checks
                           |
                           v
                     Transform Layer
            +--------------+--------------+
            |                             |
        Dimension Tables              Fact Tables
   (SCD1 / SCD2 / SCD3)              (Orders)
            |                             |
            +--------------+--------------+
                           |
                           v
                    Data Warehouse
                           |
                           v
                     Analytics Ready
```

---

# Technology Stack

| Component       | Technology            |
| --------------- | --------------------- |
| Language        | Python / Scala        |
| Data Processing | Pandas / Apache Spark |
| Build Tool      | SBT                   |
| Data Storage    | CSV (Warehouse Layer) |
| API Integration | Requests Library      |

---

# Project Structure

```
customer-orders-datawarehouse
│
├── app
│   └── main_pipeline.py
│
├── etl
│   ├── extract
│   │   └── api_ingestion.py
│   │
│   ├── transform
│   │
│   └── load
│
├── shared
│   └── data_quality.py
│
├── warehouse
│   ├── dim_customer.csv
│   ├── dim_product.csv
│   ├── dim_payment.csv
│   └── fact_orders.csv
│
├── src/main/scala
│   └── com/datawarehouse/orders/app/MainApp.scala
│
├── build.sbt
├── requirements.txt
├── run_pipeline.sh
└── README.md
```

---

# Data Sources (API Ingestion)

The pipeline ingests JSON data from the following APIs:

| Dataset   | API                                        |
| --------- | ------------------------------------------ |
| Customers | https://jsonplaceholder.typicode.com/users |
| Products  | https://dummyjson.com/products             |
| Orders    | https://dummyjson.com/carts                |
| Payments  | Simulated Payment API                      |

---

# Data Quality Checks

The pipeline validates:

* Missing values
* Empty datasets
* Schema consistency

Example validation:

```
Customers validation passed
Products validation passed
Payments validation passed
Orders validation passed
```

---

# Slowly Changing Dimensions

## SCD1 – Product Dimension

Only the **latest product price** is stored.

```
dim_product
product_id
product_name
price
```

Old values are overwritten.

---

## SCD2 – Customer Dimension

Customer address changes are **tracked historically**.

```
dim_customer
customer_id
name
address
start_date
end_date
is_current
```

Example:

| customer_id | address   | start_date | end_date | is_current |
| ----------- | --------- | ---------- | -------- | ---------- |
| 1           | Hyderabad | 2025       | 2026     | false      |
| 1           | Bangalore | 2026       | null     | true       |

---

## SCD3 – Payment Dimension

Stores **current and previous payment method**.

```
dim_payment
payment_id
current_method
previous_method
```

---

# Fact Table

```
fact_orders
order_id
customer_id
product_id
payment_id
amount
```

This table stores **transaction-level data**.

---

# Incremental Batch Processing

The pipeline implements **incremental loads**.

Each run:

```
Existing warehouse
      +
New API records
      =
Updated warehouse tables
```

Duplicate records are avoided using **primary key filtering**.

---

# Running the Pipeline

Navigate to the project directory:

```
cd customer-orders-datawarehouse
```

Run the pipeline:

```
python app/main_pipeline_pandas.py
```

---

# Warehouse Output

```
warehouse
│
├── dim_customer.csv
├── dim_product.csv
├── dim_payment.csv
└── fact_orders.csv
```

---

# Build Spark JAR

Using **SBT**

```
sbt package
```

Output:

```
target/scala-2.12/customer-orders-datawarehouse_2.12-1.0.jar

Run Data Pipeline
Publish Artifacts
```

---

# Test Scenarios

The project includes validation for:

* Missing values
* Duplicate records
* Incremental loads
* Dimension updates
* Fact table consistency

Testing framework:

```
pytest
```

---

# Future Improvements

* Delta Lake integration
* Real-time streaming with Kafka
* Cloud deployment (AWS / Azure)
* Data catalog integration

---

# Author

**Pitchala Srinivasa Reddy**

Data Engineering Portfolio Project
