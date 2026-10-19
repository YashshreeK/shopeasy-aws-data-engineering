# Bronze Layer — Raw Data Ingestion and Validation

## Overview

The Bronze layer stores raw CSV files from the Olist Brazilian E-Commerce dataset in Amazon S3. An AWS Lambda function validates incoming files before downstream processing.

## AWS Services

- **Amazon S3:** Stores raw CSV datasets.
- **AWS Lambda:** Performs file-level and schema validation.
- **Amazon CloudWatch:** Captures Lambda execution logs and validation results.

## Source Datasets

The pipeline processes nine datasets: customers, orders, order_items, payments, reviews, products, sellers, geolocation, and category_translation.

## Data Validation

The Bronze Validator Lambda performs the following checks:

1. Confirms the S3 object exists.
2. Rejects empty files.
3. Verifies the `.csv` file extension.
4. Validates UTF-8 encoding.
5. Checks required columns against predefined schemas.

The current implementation defines minimum record-count thresholds, but enforcement is disabled.

## Processing Flow

1. Raw CSV files are uploaded to the `Bronze/` prefix in Amazon S3.
2. An S3 object-created event triggers AWS Lambda.
3. Lambda identifies the dataset from the S3 object path.
4. Lambda validates the file and logs the result.
5. Validated Bronze data is available for downstream AWS Glue processing.

## Implementation

[View Bronze Validator Lambda](lambda/bronze_validator.py)

## Current Limitations

- The Lambda handler processes the first record in an S3 event.
- Record-count validation is currently disabled.
- Validation errors are returned in the Lambda response rather than raised as invocation exceptions.
