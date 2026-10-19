# Silver Layer — Data Cleansing, Transformation & Quality

## Overview

The Silver layer transforms raw e-commerce datasets from the Bronze layer into structured, analytics-ready Parquet datasets using AWS Glue and PySpark.

The pipeline implements data cleansing, schema normalization, deduplication, data quality validation, and audit tracking.

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

## Data Quality Framework

The Silver ETL jobs generate data quality fields such as:

- `dq_status` or dataset-specific validity flags
- `dq_issue_code` or `dq_error_code`
- Descriptive error messages where implemented
- Processing timestamps and source lineage attributes

Invalid records are retained with quality indicators in Silver. The Orders and Products jobs additionally write invalid records to dedicated S3 quarantine paths.

## Incremental Processing

The Orders ETL supports both FULL and INCREMENTAL load modes.

- FULL loads rebuild the complete Silver Orders dataset.
- INCREMENTAL loads filter orders by purchase date and overwrite the corresponding date partition.
- The current incremental implementation still scans the historical Bronze CSV source.

The other five documented Silver jobs perform full dataset overwrites.

## Data Quality Results

Product validation identified:

- Total product records: **32,951**
- Valid product records: **32,947**
- Invalid product records: **4**

Invalid product records were flagged with `INVALID_PRODUCT_WEIGHT`.

## Source-to-Target Validation

Data quality checks and reconciliation were performed using AWS Glue execution logs and Amazon Athena queries.

The Orders and Products scripts implement explicit count reconciliation. Other jobs implement dataset-specific validation flags.

## Implementation Scripts

- [Orders ETL](glue/orders_bronze_to_silver.py)
- [Customers ETL](glue/customers_bronze_to_silver.py)
- [Order Items ETL](glue/order_items_bronze_to_silver.py)
- [Products ETL](glue/products_bronze_to_silver.py)
- [Sellers ETL](glue/sellers_bronze_to_silver.py)
- [Reviews ETL](glue/reviews_bronze_to_silver.py)

## Implementation Notes

- Review deduplication is currently disabled in the Reviews script.
- Invalid records are not automatically excluded from Silver outputs.
- Orders and Products maintain separate quarantine datasets.
- The scripts use configurable example S3 bucket placeholders for public GitHub documentation.
