# shopeasy-aws-data-engineering
End-to-end AWS Data Engineering project implementing a Bronze–Silver–Gold architecture using S3, Lambda, Glue, PySpark, Athena, and automated data quality checks.

## Overview

ShopEasy processes nine e-commerce datasets—customers, orders, order items, payments, reviews, products, sellers, geolocation, and category translations—from source CSV files into curated Parquet datasets and analytics-ready dimensional models.

**Highlights:** source validation, distributed ETL, data-quality rules, quarantine handling, partitioned Parquet, full/incremental loading, dimensional modeling, SQL reconciliation, and customer analytics.

## Dataset Used
This project uses the Brazilian E-Commerce Public Dataset by Olist, available on Kaggle.

The dataset contains approximately 100,000 orders placed through a Brazilian e-commerce marketplace between 2016 and 2018. It includes information about customers, orders, products, sellers, payments, customer reviews, and geographical locations.

**The dataset consists of nine CSV files:**
| Dataset | Description |
| :--- | :--- |
| **Customers** | Customer identifiers and location information |
| **Orders** | Order status, purchase timestamps, and delivery dates |
| **Order Items** | Products, sellers, prices, and freight charges |
| **Payments** | Payment methods, installments, and transaction values |
| **Reviews** | Customer ratings and written reviews |
| **Products** | Product categories, dimensions, and weights |
| **Sellers** | Seller identifiers and geographical information |
| **Geolocation** | ZIP codes, cities, states, and geographical coordinates |
| **Category Translation** | Portuguese-to-English product category mappings |

The source CSV files are ingested into the AWS S3 Bronze layer, processed using AWS Glue and PySpark in the Silver layer, and transformed into dimensional models and analytical marts in the Gold layer.

**Dataset Source:** https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
## Architecture

<img width="1600" height="1986" alt="mermaid-diagram" src="https://github.com/user-attachments/assets/eda3d34b-01d7-4dd5-8e15-33e590d618ab" />


This diagram describes data flow, not a claim that all stages are controlled by a single production orchestrator.

## Pipeline layers

| Layer | Purpose | AWS services |
|---|---|---|
| **Bronze** | Preserve original CSVs; validate files and schemas | S3, Lambda, Glue Catalog |
| **Silver** | Clean, type, deduplicate, validate, quarantine flagged data; store partitioned Parquet | Glue, PySpark, S3 |
| **Gold** | Create dimensions, facts, and business reporting marts | Glue, PySpark, S3, Athena |

### Gold datasets

| Type | Tables |
|---|---|
| Dimensions | `dim_customer`, `dim_date`, `dim_location`, `dim_product`, `dim_seller` |
| Facts | `fact_order_items`, `fact_payments`, `fact_reviews` |
| Marts | `mart_customer_360`, `mart_monthly_sales`, `mart_product_category`, `mart_seller_performance` |

## Validated results

Results below were checked with Athena during development.

| Validation | Result |
|---|---:|
| Bronze orders | 99,441 |
| Silver orders / distinct IDs | 99,441 / 99,441 |
| Duplicate Silver orders | 0 |
| Silver purchase-date partitions | 634 |
| Gold order-item fact rows | 112,650 |
| Distinct orders in item fact | 98,666 |
| Gold payment fact rows | 103,886 |
| Gold review fact rows | 99,224 |
| Customer 360 rows / unique keys | 96,096 / 96,096 |
| Customer 360 DQ-valid / invalid | 96,096 / 0 |
| Customers without item-level purchases | 676 |

### Why do 775 orders have no item fact rows?

The Silver order-header dataset contains **99,441** distinct orders, while `fact_order_items` represents **98,666**. We verified that the remaining **775** orders have **no corresponding item records in the original Bronze order-items dataset**:

| Status | Orders without items |
|---|---:|
| Unavailable | 603 |
| Canceled | 164 |
| Created | 5 |
| Invoiced | 2 |
| Shipped | 1 |
| **Total** | **775** |

This is a source-data/grain distinction, not evidence of missing item records introduced by the Gold transformation.

### Customer 360 business rules

Customer 360 combines `dim_customer` with item-level fact aggregates. Customers with order history but no item-level purchases are retained, have zero purchase revenue, and are assigned **`No Purchases`** rather than `One-Time`. Zero purchases alone do not fail the current DQ rule.

After updating the Glue job and refreshing the catalog, **all 96,096 customers passed the implemented DQ checks**, including the **676** customers without item-level purchases. This does not imply that every possible data-quality issue has been tested.

### Revenue reconciliation

The 24-month monthly-sales mart was reconciled to the item fact:

| Measure | Amount |
|---|---:|
| Item revenue | $13,591,643.70 |
| Freight | $2,251,909.54 |
| Total item value plus freight | $15,843,553.24 |

## Engineering decisions

- **Full and incremental Silver loads:** The orders Glue job supports `FULL` and `INCREMENTAL` modes with appropriate partition-overwrite behavior.
- **Storage optimization:** Silver orders use date-partitioned, Snappy-compressed Parquet.
- **Idempotency:** Orders are deduplicated by `order_id` and written using overwrite semantics.
- **Data quality:** Timestamp-sequence and dataset-specific checks generate DQ flags; flagged records can also be written to quarantine.
- **Catalog and query layer:** Glue crawlers register tables and partitions for Athena.
- **Reconciliation:** Source-to-target row counts, uniqueness, key joins, and monetary totals are validated independently.
- **Business-aware modeling:** Order headers, item transactions, and customer purchase activity are treated as distinct concepts.

## Repository layout

```text
shopeasy-aws-data-engineering/
├── README.md
├── architecture/        # Diagrams and design notes
├── bronze/              # Bronze validation Lambda and related code
├── silver/              # Glue / PySpark transformations
├── gold/                # Dimensions, facts, and analytical marts
├── orchestration/       # Workflow definitions, if added
├── sql/
│   └── validation/      # Athena reconciliation and DQ SQL
└── docs/                # Data dictionary, runbook, implementation notes
```

*This is the planned repository structure; directories and code will be added as they are prepared for publication.*

## Example Athena checks

```sql
SELECT
    COUNT(*) AS total_orders,
    COUNT(DISTINCT order_id) AS unique_orders,
    COUNT(*) - COUNT(DISTINCT order_id) AS duplicate_orders
FROM shopeasy_silver_dev.orders;
```

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

## Reproducing the project

This repository documents an AWS implementation rather than offering a one-command deployment.

1. Provision your own S3 bucket and IAM roles with least-privilege permissions.
2. Obtain source datasets you are authorized to use.
3. Configure Bronze, Silver, Gold, and Quarantine storage prefixes.
4. Parameterize resource names, database names, and paths in the scripts.
5. Run Bronze validation, then Silver ETL, then Gold transformations.
6. Refresh Glue Catalog tables and partitions.
7. Run Athena validation queries and reconcile results against your own data.

**Security:** Do not commit AWS credentials, account IDs, personal information, private infrastructure identifiers, or secrets. Replace account-specific paths with placeholders or configuration variables before publishing.

## Key lessons

- **Understand grain:** Order headers without order items cannot appear in an item-grain fact table.
- **Validate partitions:** A successful Glue run does not prove every historical partition was written and cataloged.
- **Make DQ business-aware:** A customer with no item-level purchases is not inherently invalid.
- **Reconcile end to end:** Source counts, fact counts, mart metrics, and DQ outcomes should be independently checked.

## Status

**Implemented and validated:** Bronze validation, Silver processing, Gold dimensions/facts/marts, and Athena-based reconciliation.

**GitHub preparation:** Documentation and code are being reviewed and sanitized before publication.

## License

No license selected yet.
