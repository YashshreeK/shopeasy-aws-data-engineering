# Gold Layer — Analytics and Data Marts

## Overview

The Gold layer is the analytics-ready layer of the ShopEasy AWS Data Engineering project. It transforms cleansed Silver datasets into dimensional models, transaction-level fact tables, and aggregated business data marts.

**Technologies:** AWS Glue, PySpark, Amazon S3, AWS Glue Data Catalog, Amazon Athena

**Data source:** [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## Architecture

```text
Silver Layer (Parquet)
         |
         v
AWS Glue PySpark ETL
         |
         +-- Dimensions (5)
         |   +-- dim_date
         |   +-- dim_customer
         |   +-- dim_product
         |   +-- dim_location
         |   +-- dim_seller
         |
         +-- Fact Tables (3)
         |   +-- fact_order_items
         |   +-- fact_payments
         |   +-- fact_reviews
         |
         +-- Data Marts (4)
             +-- mart_customer_360
             +-- mart_monthly_sales
             +-- mart_product_category
             +-- mart_seller_performance
         |
         v
Amazon S3 — Gold Parquet
         |
         v
AWS Glue Data Catalog
         |
         v
Amazon Athena / SQL Analytics
```

## Gold Dimensions

| Dimension | Description | Records |
|---|---|---:|
| `dim_date` | Calendar attributes for reporting | 800 |
| `dim_customer` | Customer identity, latest location and purchase history | 96,096 |
| `dim_product` | Product details and translated category names | 32,951 |
| `dim_location` | Canonical location by ZIP prefix | 19,016 |
| `dim_seller` | Seller attributes and resolved location | 3,095 |

Location data is aggregated at ZIP-prefix grain. The most frequent city/state combination is selected, and a special Unknown Location member uses `location_key = -1`.

## Gold Fact Tables

| Fact Table | Grain | Records |
|---|---|---:|
| `fact_order_items` | Order item | 112,650 |
| `fact_payments` | Payment sequence per order | 103,886 |
| `fact_reviews` | Silver review record | 99,224 |

Fact tables resolve surrogate keys against Gold dimensions, retain relevant transaction attributes and measures, and include data quality and lineage columns.

## Gold Data Marts

### Customer 360

Combines customer dimensions with order-item transactions to calculate:

- Lifetime revenue and freight
- Order frequency and average order value
- Customer recency and tenure
- Late-delivery metrics
- Customer frequency, value and engagement segments

**Records:** 96,096

### Monthly Sales

Aggregates order-item transactions by calendar month.

Metrics include revenue, freight, distinct orders, customer counts, average order value and late-delivery percentage.

**Records:** 24

### Product Category Performance

Aggregates sales by reporting product category.

Metrics include item revenue, freight, distinct orders, customer and seller counts, revenue contribution, category ranking and product quality indicators.

**Records:** 74

### Seller Performance

Combines seller dimensions with order-item transactions.

Metrics include seller revenue, order volume, customer reach, product diversity, delivery performance, revenue ranking and performance segmentation.

**Records:** 3,095

## ETL Execution Order

Gold ETL jobs have dependencies and should execute in the following order:

1. Build dimensions from Silver sources:
   - `dim_date`
   - `dim_customer`
   - `dim_product`
   - `dim_location`
2. Build `dim_seller` after `dim_location` is available.
3. Register the dimension tables in the Glue Data Catalog.
4. Build the fact tables:
   - `fact_order_items`
   - `fact_payments`
   - `fact_reviews`
5. Register fact tables in the Glue Data Catalog.
6. Build the data marts:
   - `mart_customer_360`
   - `mart_monthly_sales`
   - `mart_product_category`
   - `mart_seller_performance`

Independent jobs within the same dependency stage may run in parallel.

## Data Quality and Reliability

The Gold layer implements:

- Deterministic surrogate keys using `xxhash64`
- Dimension lookups and foreign-key validation
- Source and Gold-level data quality flags
- Unknown location handling with `location_key = -1`
- Duplicate-aware transaction aggregation using `countDistinct`
- Audit timestamps and source lineage
- Full-refresh Parquet writes using overwrite mode

Records failing Gold data quality checks are generally retained with flags rather than silently removed.

## Validation Highlights

The following results were recorded during project validation:

| Metric | Result |
|---|---:|
| Silver distinct orders | 99,441 |
| Gold order-item records | 112,650 |
| Gold distinct orders with items | 98,666 |
| Customer 360 valid records | 96,096 |
| Customer 360 invalid records | 0 |
| Customers classified as No Purchases | 676 |
| Monthly sales reporting periods | 24 |
| Total item revenue | $13,591,643.70 |
| Total freight | $2,251,909.54 |
| Total item value including freight | $15,843,553.24 |

The difference between total Silver orders and orders represented in `fact_order_items` reflects orders without corresponding source order-item records, rather than an ETL data loss.

## Repository Structure

```text
gold/
├── dimensions/
│   ├── dim_date.py
│   ├── dim_customer.py
│   ├── dim_product.py
│   ├── dim_location.py
│   └── dim_seller.py
├── facts/
│   ├── fact_order_items.py
│   ├── fact_payments.py
│   └── fact_reviews.py
├── marts/
│   ├── mart_customer_360.py
│   ├── mart_monthly_sales.py
│   ├── mart_product_category.py
│   └── mart_seller_performance.py
└── README.md
```

## Implementation Notes

The scripts use `YOUR_SHOPEASY_BUCKET` as a placeholder for the Amazon S3 bucket name. Replace this value with your own bucket when deploying.

Gold scripts require their upstream Silver and Gold datasets to be registered in the AWS Glue Data Catalog before execution.

Revenue metrics in the order-item marts represent item prices and freight, not payment receipts. Seller segmentation thresholds are project-defined business rules.

## Business Value

The Gold layer enables analytics for customer behavior, sales trends, product category performance, seller operations and delivery quality.

It demonstrates dimensional modeling, PySpark transformations, AWS Glue development, data quality validation, referential integrity, analytical aggregation and reusable cloud data engineering patterns.
