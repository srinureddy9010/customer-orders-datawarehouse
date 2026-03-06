# Pipeline Output Examples

## Sample Output

When running `python app/main_pipeline_pandas.py`, the pipeline produces the following:

### Console Output

```
==================================================
Starting Data Warehouse Pipeline (Pandas Version)
==================================================

Customers Data
 customer_id  name   address
           1  Ravi Hyderabad
           2 Anita     Delhi


Products Data
 product_id product_name  price
        101       Laptop  50000
        102       Mobile  20000


Payments Data
 payment_id      method
          1         UPI
          2 Credit Card


Orders Data
 order_id  customer_id  product_id  payment_id  amount
        1            1         101           1   50000
        2            2         102           2   20000

==================================================
APPLYING TRANSFORMATIONS
==================================================


Applying SCD2 for customer dimension → ./warehouse/dim_customer
  Created SCD2 table: ./warehouse/dim_customer.csv

Applying SCD1 for product dimension → ./warehouse/dim_product
  Created SCD1 table: ./warehouse/dim_product.csv

Applying SCD3 for payment dimension → ./warehouse/dim_payments
  Created SCD3 table: ./warehouse/dim_payments.csv

Creating fact orders table

Created fact table: ./warehouse/fact_orders.csv

==================================================
✓ Warehouse tables created successfully!
==================================================


Warehouse Structure:
--------------------------------------------------
  dim_customer.csv               (156 bytes)
  dim_payments.csv               (81 bytes)
  dim_product.csv                (67 bytes)
  fact_orders.csv                (85 bytes)
```

## Generated Tables

### 1. dim_customer.csv (SCD Type 2)

**Purpose**: Tracks all customer dimension changes with effective dating

```csv
customer_id,name,address,start_date,end_date,is_current
1,Ravi,Hyderabad,2026-03-06 08:39:48.643612,,True
2,Anita,Delhi,2026-03-06 08:39:48.643612,,True
```

**Key Columns**:
- `customer_id` - Surrogate key
- `name`, `address` - Customer attributes
- `start_date` - When the record became active
- `end_date` - When the record became inactive (NULL for current)
- `is_current` - Boolean flag for current record

**Use Case**: 
- Historical analysis: "What was customer's address on date X?"
- Change tracking: "When did customer move to a new address?"

### 2. dim_product.csv (SCD Type 1)

**Purpose**: Maintains latest product data only (no history)

```csv
product_id,product_name,price
101,Laptop,50000
102,Mobile,20000
```

**Key Columns**:
- `product_id` - Product identifier
- `product_name` - Product name
- `price` - Current price

**Characteristics**:
- Simple structure (1 row per product)
- Previous values are overwritten
- Fast queries

**Use Case**: 
- Current product catalog
- Product lookups
- When historical price is not needed

### 3. dim_payments.csv (SCD Type 3)

**Purpose**: Tracks current and previous payment methods

```csv
payment_id,current_method,previous_method
1,UPI,UPI
2,Credit Card,Credit Card
```

**Key Columns**:
- `payment_id` - Payment method identifier
- `current_method` - Current payment method
- `previous_method` - Previous payment method

**Characteristics**:
- Tracks current + 1 previous version
- Efficient storage
- Good for recent change analysis

**Use Case**:
- "What payment method did we switch from?"
- Minimal storage for change tracking

### 4. fact_orders.csv (Fact Table)

**Purpose**: Central fact table capturing order transactions

```csv
order_id,customer_id,product_id,payment_id,amount
1,1,101,1,50000
2,2,102,2,20000
```

**Key Columns**:
- `order_id` - Unique order identifier
- `customer_id` - Foreign key to dim_customer
- `product_id` - Foreign key to dim_product
- `payment_id` - Foreign key to dim_payments
- `amount` - Order amount (measure)

**Characteristics**:
- One row per order transaction
- Contains foreign keys to dimensions
- Contains measures (amount)
- Fact grain: Order level

**Use Cases**:
- Revenue analysis
- Order trend analysis
- Customer purchase patterns
- Product sales analysis

## Data Warehouse Schema

```
                        dim_customer (SCD2)
                              |
                              | FK: customer_id
                              |
    fact_orders ◄─────────────┘
        |
        ├──────────────────► dim_product (SCD1)
        |                       FK: product_id
        |
        └──────────────────► dim_payments (SCD3)
                               FK: payment_id
```

## Data Quality Validation Results

```
============================================================
RUNNING DATA QUALITY VALIDATION
============================================================

✓ Null check passed: 'customer_id' has no null values
✓ Null check passed: 'name' has no null values
✓ Duplicate check passed: 'customer_id' has no duplicates

✓ Null check passed: 'product_id' has no null values
✓ Null check passed: 'product_name' has no null values
✓ Duplicate check passed: 'product_id' has no duplicates
✓ Range check passed: All values in 'price' are within [0, 10000000]

✓ Null check passed: 'order_id' has no null values
✓ Null check passed: 'customer_id' has no null values
✓ Duplicate check passed: 'order_id' has no duplicates
✓ Range check passed: All values in 'amount' are within [0, 100000000]

✓ Null check passed: 'payment_id' has no null values
✓ Duplicate check passed: 'payment_id' has no duplicates

✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓
✓ ALL DATA QUALITY CHECKS PASSED!
✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓✓
```

## Technology Stack Used

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Language** | Python 3.8+ | Programming language |
| **ETL** | Pandas | Data manipulation |
| **Data Source** | JSON | Raw input format |
| **Storage** | CSV | Warehouse output |
| **Version Control** | Git | Source control |

## Performance Metrics

- **Execution Time**: ~2 seconds
- **Records Processed**: 6 total (2 customers, 2 products, 2 orders, 2 payments)
- **Output Size**: ~389 bytes total
- **Data Quality Checks**: 13 validations passed

## Next Steps for Production

To scale this pipeline to production:

1. **Incremental Loading**: Implement CDC (Change Data Capture) for updates
2. **Error Handling**: Add try-except with logging
3. **Scheduling**: Use Apache Airflow for orchestration
4. **Monitoring**: Add metrics and alerting
5. **Database**: Load to SQL warehouse (Snowflake, Redshift, BigQuery)
6. **Testing**: Comprehensive unit and integration tests
7. **Documentation**: Data dictionary and lineage mapping
