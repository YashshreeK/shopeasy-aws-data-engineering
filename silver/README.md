# Silver Layer — Data Cleansing, Transformation & Quality

## Overview

The Silver layer transforms raw e-commerce datasets from the Bronze layer into structured, analytics-ready Parquet datasets using AWS Glue and PySpark.

The pipeline processes **nine e-commerce datasets**, implementing data cleansing, schema normalization, deduplication where applicable, data quality validation, and audit tracking.

## Technologies

- AWS Glue ETL
- Apache Spark / PySpark
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- Parquet with Snappy compression

## ETL Jobs

| Dataset | Transformations | Data Quality Rules |
|---|---|---|
| Orders | Timestamp conversion, order ID deduplication, date partitioning, full/incremental loading | Invalid timestamp sequences |
| Customers | String cleansing, location standardization, customer ID deduplication | Missing customer identifiers and location |
| Order Items | Numeric/timestamp casting, composite-key deduplication, item total calculation | Missing IDs, invalid prices, freight, and shipping dates |
| Products | Product measurement casting and normalization | Missing or nonpositive product weight |
| Sellers | Seller ID deduplication and location standardization | Missing seller identifiers and location |
| Reviews | Multiline CSV parsing, timestamp conversion, comment indicators | Missing identifiers, invalid review scores, invalid review timestamps |
| Payments | Payment type normalization, numeric casting, deduplication using order ID and payment sequence | Missing order IDs, invalid payment sequence, missing payment type, invalid installments, negative or missing payment values |
| Category Translation | Portuguese and English category normalization, lowercase conversion, category-key deduplication | Missing category names and missing English translations |
| Geolocation | ZIP prefix conversion, city/state normalization, latitude/longitude casting | Missing ZIP, city or state; invalid latitude or longitude ranges |

## Data Quality Framework

The Silver ETL jobs generate data quality fields such as:

- `dq_status` or dataset-specific validity flags
- `dq_issue_code` or `dq_error_code`
- Descriptive error messages where implemented
- Processing timestamps and source lineage attributes

Additional dataset-specific validity fields include:

- `dq_payment_valid`
- `dq_category_valid`
- `dq_geolocation_valid`

Invalid records are retained with quality indicators in Silver.

The Orders and Products jobs additionally write invalid records to dedicated S3 quarantine paths.

For Payments, Category Translation, and Geolocation, `dq_issue_code` records the first matching validation issue according to the rule priority defined in each script.

## Incremental Processing

The Orders ETL supports both FULL and INCREMENTAL load modes.

- FULL loads rebuild the complete Silver Orders dataset.
- INCREMENTAL loads filter orders by purchase date and overwrite the corresponding date partitions.
- The current incremental implementation still scans the historical Bronze CSV source.

The other eight Silver ETL jobs perform full dataset overwrites.

## Deduplication Strategy

Deduplication is applied according to each dataset's business keys:

- Orders: `order_id`
- Customers: `customer_id`
- Order Items: Composite order-item business key
- Sellers: `seller_id`
- Payments: `order_id` and `payment_sequential`
- Category Translation: `product_category_name`

Review deduplication is currently disabled.

Geolocation records are not deduplicated in the Silver ETL. Multiple geolocation records may share the same ZIP code prefix.

## Data Quality Results

Product validation identified:

- Total product records: **32,951**
- Valid product records: **32,947**
- Invalid product records: **4**

Invalid product records were flagged with `INVALID_PRODUCT_WEIGHT`.

These figures represent recorded validation results for the Products dataset, not an aggregate quality summary across all nine datasets.

## Source-to-Target Validation

Data quality checks and reconciliation were performed using AWS Glue execution logs and Amazon Athena queries.

The Orders and Products scripts implement explicit count reconciliation.

Other jobs implement dataset-specific validation flags.

The Payments, Category Translation, and Geolocation scripts write processed datasets as Snappy-compressed Parquet and preserve audit attributes:

- `silver_processed_at`
- `source_layer`
- `source_dataset`

## Implementation Scripts

- [Orders ETL](glue/orders_bronze_to_silver.py)
- [Customers ETL](glue/customers_bronze_to_silver.py)
- [Order Items ETL](glue/order_items_bronze_to_silver.py)
- [Products ETL](glue/products_bronze_to_silver.py)
- [Sellers ETL](glue/sellers_bronze_to_silver.py)
- [Reviews ETL](glue/reviews_bronze_to_silver.py)
- [Payments ETL](glue/payments_bronze_to_silver.py)
- [Category Translation ETL](glue/category_translation_bronze_to_silver.py)
- [Geolocation ETL](glue/geolocation_bronze_to_silver.py)

## Implementation Notes

- Review deduplication is currently disabled in the Reviews script.
- Invalid records are not automatically excluded from Silver outputs.
- Orders and Products maintain separate quarantine datasets.
- Payments, Category Translation, and Geolocation retain invalid records with DQ flags but do not write separate quarantine datasets.
- Geolocation retains multiple records for the same ZIP prefix.
- Most Silver jobs use full overwrites; only Orders implements an incremental loading mode.
- The public GitHub scripts use example S3 bucket placeholders instead of private account-specific bucket names.
- AWS Glue Data Catalog tables provide schema metadata for querying Silver datasets using Amazon Athena.
