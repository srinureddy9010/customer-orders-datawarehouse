# Customer Orders Data Warehouse

## Project Overview

This project implements a **Mini Data Warehouse Pipeline** that processes customer orders data using **batch processing** and implements **Slowly Changing Dimensions (SCD1, SCD2, SCD3)**.

The pipeline ingests data from **multiple APIs**, performs **data quality checks**, applies **dimension transformations**, and generates **fact and dimension tables** for analytics.

This project demonstrates common **data engineering practices** including:

* API Data Ingestion
* Data Quality Validation
* Slowly Changing Dimensions (SCD1, SCD2, SCD3)
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
| Language        | Python                |
| Data Processing | Pandas                |
| API Integration | Requests              |
| Data Storage    | CSV (Warehouse Layer) |
| Version Control | Git                   |
| Repository      | GitHub                |

---

# Project Structure

```
customer-orders-datawarehouse
│
├── app
│   └── main_pipeline_pandas.py
│
├── etl
│   └── extract
│       └── api_ingestion.py
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
├── requirements.txt
└── README.md
```

---

# Data Sources (API Ingestion)

The pipeline extracts JSON data from the following APIs:

| Dataset   | API                                        |
| --------- | ------------------------------------------ |
| Customers | https://jsonplaceholder.typicode.com/users |
| Products  | https://dummyjson.com/products             |
| Orders    | https://dummyjson.com/carts                |
| Payments  | Simulated payment dataset                  |

---

# Data Quality Checks

The pipeline validates the datasets before transformation.

Checks include:

* Empty dataset validation
* Null value detection
* Basic schema validation

Example output:

```
Customers validation passed
Products validation passed
Payments validation passed
Orders validation passed
```

---

# Slowly Changing Dimensions

## SCD1 – Product Dimension

Stores **only the latest product price**.

```
dim_product
product_id
product_name
price
```

Old values are overwritten.

---

## SCD2 – Customer Dimension

Tracks **historical address changes** for customers.

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

The fact table stores **transaction-level order data**.

```
fact_orders
order_id
customer_id
product_id
payment_id
amount
```

This table connects all dimension tables for analytics.

---

# Incremental Batch Processing

The pipeline supports **incremental loading**.

Each run:

```
Existing warehouse data
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

After running the pipeline, the warehouse folder will contain:

```
warehouse
│
├── dim_customer.csv
├── dim_product.csv
├── dim_payment.csv
└── fact_orders.csv
```

---

# Test Scenarios

The pipeline supports validation for:

* Missing values
* Duplicate records
* Dimension updates
* Fact table consistency

Testing framework used:

```
pytest
```

---

# Author

**Pitchala Srinivasa Reddy**

Data Engineering Portfolio Project
