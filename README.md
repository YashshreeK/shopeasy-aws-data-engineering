# ShopEasy — End-to-End AWS Data Engineering Pipeline

**AWS S3 | AWS Lambda | AWS Glue | PySpark | Amazon Athena | Data Quality | Dimensional Modeling**

An end-to-end AWS Data Engineering project implementing a **Bronze–Silver–Gold Medallion Architecture** to transform raw e-commerce data into validated, analytics-ready datasets.

The project demonstrates cloud-native data ingestion, distributed ETL processing, data quality validation, dimensional modeling, incremental loading, reconciliation, and business analytics.

## Project Highlights

| Metric | Result |
|---|---:|
| Source datasets | 9 |
| Published pipeline scripts | 22 |
| Silver ETL jobs | 9 |
| Gold dimensions | 5 |
| Gold fact tables | 3 |
| Gold analytical marts | 4 |
| Silver orders processed | 99,441 |
| Order-item fact records | 112,650 |
| Customer 360 records | 96,096 |
| Customer 360 DQ-valid records | 96,096 |
| Reconciled item and freight value | $15.84M |

## Architecture

![ShopEasy AWS Data Engineering Architecture](https://github.com/user-attachments/assets/eda3d34b-01d7-4dd5-8e15-33e590d618ab)

The architecture follows three logical processing layers:

**Bronze → Silver → Gold → Amazon Athena**

The diagram illustrates data flow and dependencies. It does not imply that all jobs are managed by a single production orchestrator.

For more details, see [Architecture Documentation](architecture/README.md).

## Technology Stack

| Technology | Purpose |
|---|---|
| Amazon S3 | Raw, cleansed, curated, and quarantine data storage |
| AWS Lambda | Event-driven Bronze file validation |
| AWS Glue | Serverless ETL processing |
| Apache Spark / PySpark | Distributed transformations and aggregations |
| AWS Glue Data Catalog | Table schemas, metadata, and partitions |
| Amazon Athena | SQL validation, reconciliation, and analytics |
| Python | Lambda validation and pipeline development |
| Parquet / Snappy | Columnar data storage and compression |
| IAM | Service permissions and access control |

## Dataset

The project uses the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), containing approximately 100,000 marketplace orders from 2016–2018.

Nine source CSV datasets are processed:

| Dataset | Description |
|---|---|
| Customers | Customer identity and location |
| Orders | Order status and lifecycle timestamps |
| Order Items | Purchased products, sellers, prices, and freight |
| Payments | Payment methods, installments, and values |
| Reviews | Customer ratings and comments |
| Products | Product attributes and categories |
| Sellers | Seller identity and location |
| Geolocation | ZIP prefixes and geographic coordinates |
| Category Translation | Portuguese-to-English category mappings |

## Medallion Architecture Implementation

### Bronze Layer — Raw Ingestion and Validation

The Bronze layer preserves source CSV files in Amazon S3.

An S3-triggered AWS Lambda function validates incoming datasets, including expected file names, required columns, and file readability.

**Key capabilities:**

- Event-driven file validation
- Dataset-specific schema checks
- Source file integrity checks
- Validation logging and error handling
- Glue Data Catalog integration

**Implementation:** [Bronze Layer](bronze/README.md)

### Silver Layer — Data Cleansing and Transformation

The Silver layer uses AWS Glue and PySpark to transform raw CSV datasets into structured Parquet files.

Nine Glue ETL scripts implement dataset-specific transformation and validation logic.

**Key capabilities:**

- Schema standardization and type conversion
- Null and duplicate handling
- Business-specific data quality rules
- Date and timestamp validation
- Quarantine handling for selected invalid records
- Snappy-compressed Parquet output
- Date partitioning for Silver orders
- Full and incremental processing modes for orders
- Idempotent overwrite behavior

**Implementation:** [Silver Layer](silver/README.md)

### Gold Layer — Dimensional Modeling and Analytics

The Gold layer transforms Silver datasets into reusable analytical models.

It includes **5 dimensions, 3 fact tables, and 4 business data marts**.

#### Dimensions

| Table | Records |
|---|---:|
| `dim_customer` | 96,096 |
| `dim_date` | 800 |
| `dim_location` | 19,016 |
| `dim_product` | 32,951 |
| `dim_seller` | 3,095 |

#### Fact Tables

| Table | Grain | Records |
|---|---|---:|
| `fact_order_items` | Order item | 112,650 |
| `fact_payments` | Payment sequence per order | 103,886 |
| `fact_reviews` | Silver review record | 99,224 |

#### Analytical Data Marts

| Data Mart | Business Purpose | Records |
|---|---|---:|
| `mart_customer_360` | Customer purchase behavior and segmentation | 96,096 |
| `mart_monthly_sales` | Monthly sales and delivery performance | 24 |
| `mart_product_category` | Product category revenue and ranking | 74 |
| `mart_seller_performance` | Seller revenue, ranking, and delivery performance | 3,095 |

**Implementation:** [Gold Layer](gold/README.md)

## Data Quality and Reconciliation

Data quality checks are implemented across Bronze, Silver, and Gold.

The project validates file schemas, transaction keys, duplicate records, timestamp sequences, dimension relationships, and business metrics.

### Order Reconciliation

| Validation | Result |
|---|---:|
| Bronze orders | 99,441 |
| Silver orders | 99,441 |
| Distinct Silver order IDs | 99,441 |
| Duplicate Silver orders | 0 |
| Silver purchase-date partitions | 634 |
| Distinct orders in `fact_order_items` | 98,666 |

**Why do 775 orders have no order-item fact records?**

The source order-header dataset contains 99,441 orders, but only 98,666 have corresponding order-item records.

The remaining 775 were verified against the original Bronze order-items dataset:

| Order Status | Count |
|---|---:|
| Unavailable | 603 |
| Canceled | 164 |
| Created | 5 |
| Invoiced | 2 |
| Shipped | 1 |
| **Total** | **775** |

This is a source-data and transaction-grain distinction, not a loss introduced by the Gold ETL process.

### Customer 360 Validation

| Validation | Result |
|---|---:|
| Customer records | 96,096 |
| Unique customer keys | 96,096 |
| DQ-valid records | 96,096 |
| DQ-invalid records | 0 |
| Customers without item-level purchases | 676 |

Customers without item-level purchases are retained with zero purchase revenue and classified as **No Purchases**.

These results reflect the implemented data quality rules rather than an exhaustive guarantee of data correctness.

### Revenue Reconciliation

The Monthly Sales Mart was reconciled against the order-item fact table.

| Metric | Amount |
|---|---:|
| Gross item revenue | $13,591,643.70 |
| Freight value | $2,251,909.54 |
| **Total order-item value** | **$15,843,553.24** |

The reporting period covers 24 calendar months. Revenue metrics represent item prices and freight rather than payment receipts.

## Engineering Design Decisions

**Incremental processing:** The Silver Orders Glue job supports FULL and INCREMENTAL processing modes with partition-overwrite semantics.
### Silver Orders Glue Job Parameters

The Silver Orders job requires the following AWS Glue job arguments:

| Argument | Description | Example |
|---|---|---|
| `--JOB_NAME` | Glue job name | `shopeasy-silver-orders` |
| `--process_date` | Purchase date to process | `2018-01-15` |
| `--load_type` | Processing mode | `FULL` or `INCREMENTAL` |

Example incremental execution:

```text
--process_date = 2018-01-15
--load_type = INCREMENTAL
```

For a full refresh, set `--load_type` to `FULL`. The `--process_date` argument must still be supplied because it is required by the current script.

Configure the S3 bucket placeholder and ensure the Glue execution role has the necessary permissions before running the job.

**Idempotency:** Order-level deduplication and overwrite-based output handling reduce duplication during reprocessing.

**Data quality:** Source and Gold validation flags preserve visibility into invalid records. Selected Silver jobs also write quarantine outputs.

**Dimensional modeling:** Deterministic surrogate keys, fact-level foreign-key lookups, and an Unknown Location member support reusable analytical joins.

**Historical reporting:** Customer recency is calculated relative to the dataset's latest transaction date rather than the current date.

**Business-aware reconciliation:** Order headers, order items, payments, and reviews are treated as separate analytical grains.

**Storage efficiency:** Parquet and Snappy compression support efficient analytical queries.

## Repository Structure

```text
shopeasy-aws-data-engineering/
├── README.md
├── architecture/
│   ├── README.md
│   └── shopeasy-architecture.md
├── bronze/
│   ├── README.md
│   └── lambda/
│       └── bronze_validator.py
├── silver/
│   ├── README.md
│   └── glue/
│       └── 9 Silver ETL scripts
└── gold/
    ├── README.md
    ├── dimensions/
    │   └── 5 dimension scripts
    ├── facts/
    │   └── 3 fact scripts
    └── marts/
        └── 4 analytical mart scripts
```

The repository contains the pipeline code and documentation. SQL validation files, automated orchestration definitions, and deployment infrastructure can be added as future enhancements.

## Example Athena Validation Queries

**Check Silver order uniqueness:**

```sql
SELECT
    COUNT(*) AS total_orders,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_orders
FROM shopeasy_silver_dev.orders;
```

**Validate Customer 360:**

```sql
SELECT
    COUNT(*) AS total_customers,
    COUNT(DISTINCT customer_key) AS unique_customers,
    COUNT_IF(dq_customer_360_valid = false) AS invalid_customers,
    COUNT_IF(
        orders_from_fact = 0
        AND customer_segment = 'No Purchases'
    ) AS no_purchase_customers
FROM shopeasy_gold_dev.mart_customer_360;
```

## Reproducing the Project

This repository documents an AWS implementation rather than providing a one-command deployment.

1. Create an Amazon S3 bucket and configure the required IAM roles.
2. Download the Olist datasets from Kaggle.
3. Configure the Bronze, Silver, Gold, and Quarantine S3 prefixes.
4. Replace `YOUR_SHOPEASY_BUCKET` and other environment-specific configuration values.
5. Run Bronze validation and the nine Silver Glue transformations.
6. Register the resulting datasets in the AWS Glue Data Catalog.
7. Execute Gold dimension jobs in dependency order, followed by fact tables and data marts.
8. Refresh Glue metadata and validate outputs using Athena.

See the individual layer READMEs for implementation details.

**Security:** AWS credentials, secrets, account IDs, and private infrastructure identifiers should never be committed to this repository.

## Key Learning Outcomes

This project demonstrates practical experience with:

- AWS-based ETL architecture and data lake development
- Distributed data processing using PySpark
- Batch ingestion, validation, and transformation
- Dimensional data modeling and analytical data marts
- Incremental processing and idempotent data loads
- Data quality frameworks and reconciliation
- SQL-based analytical validation
- Debugging data inconsistencies across processing layers

## Project Status

**Implemented:** Bronze validation, nine Silver ETL pipelines, five Gold dimensions, three fact tables, four analytical marts, and Athena-based reconciliation.

**Published:** Pipeline scripts, architecture documentation, and Bronze/Silver/Gold layer documentation.

**Potential enhancements:** Automated job orchestration, CI/CD, infrastructure as code, automated testing, and operational monitoring.

## License

No license has been selected for this repository.
