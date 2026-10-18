# ShopEasy AWS Data Engineering Architecture

This diagram illustrates the end-to-end Bronze–Silver–Gold data pipeline implemented using AWS services.


```mermaid
flowchart TD
    A["E-Commerce Source Data"]
    B["Amazon S3 - Bronze"]
    C["AWS Lambda - Validation"]
    D["AWS Glue - PySpark"]
    E["Amazon S3 - Silver"]
    F["Amazon S3 - Gold"]
    G["Amazon Athena"]

    A --> B
    B --> C
    B --> D
    D --> E
    E --> F
    F --> G
```


## Architecture Components

- **Bronze:** Raw CSV ingestion and AWS Lambda validation.
- **Silver:** AWS Glue PySpark ETL, cleansing, data-quality checks, and partitioned Parquet storage.
- **Quarantine:** Stores flagged records for investigation.
- **Gold:** Dimensional models, transaction facts, and analytics marts.
- **AWS Glue Data Catalog:** Metadata management and partition discovery.
- **Amazon Athena:** SQL analytics, data validation, and reconciliation.
