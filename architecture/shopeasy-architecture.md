# ShopEasy AWS Data Engineering Architecture

This diagram illustrates the end-to-end Bronze–Silver–Gold data pipeline implemented using AWS services.

```mermaid
flowchart TD
    A["E-Commerce Source Data<br/>9 CSV Datasets"]
    B["Amazon S3 - Bronze<br/>Raw CSV Storage"]
    C["AWS Lambda<br/>Bronze Data Validation"]
    D["AWS Glue / PySpark<br/>Cleaning and Transformations"]
    E["Amazon S3 - Silver<br/>Partitioned Parquet"]
    Q["Amazon S3 - Quarantine<br/>Flagged Records"]
    F["AWS Glue / PySpark<br/>Gold ETL"]
    G["Amazon S3 - Gold<br/>5 Dimensions + 3 Facts"]
    H["Analytics Marts<br/>Customer 360, Monthly Sales<br/>Product Category, Seller Performance"]
    I["AWS Glue Data Catalog<br/>Crawlers and Metadata"]
    J["Amazon Athena<br/>SQL Analytics and Data Quality"]

    A --> B
    B --> C
    B --> D
    D --> E
    D --> Q
    E --> F
    F --> G
    G --> H

    B -.-> I
    E -.-> I
    G -.-> I
    H -.-> I
    I --> J

    style B fill:#FFF0D8,color:#222,stroke:#D97706
    style E fill:#DCECFB,color:#222,stroke:#2563EB
    style G fill:#DCFCE7,color:#222,stroke:#16A34A
    style H fill:#DCFCE7,color:#222,stroke:#16A34A
```

## Architecture Components

- **Bronze:** Raw CSV ingestion and AWS Lambda validation.
- **Silver:** AWS Glue PySpark ETL, cleansing, data-quality checks, and partitioned Parquet storage.
- **Quarantine:** Stores flagged records for investigation.
- **Gold:** Dimensional models, transaction facts, and analytics marts.
- **AWS Glue Data Catalog:** Metadata management and partition discovery.
- **Amazon Athena:** SQL analytics, data validation, and reconciliation.
