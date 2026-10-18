import boto3

import csv

import io

import json

import logging

logger = logging.getLogger()

logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

# ---------------------------------------------------------

# Configuration

# ---------------------------------------------------------

EXPECTED_SCHEMAS = {

    "customers": {

        "customer_id",

        "customer_unique_id",

        "customer_zip_code_prefix",

        "customer_city",

        "customer_state"

    },

    "orders": {

        "order_id",

        "customer_id",

        "order_status",

        "order_purchase_timestamp",

        "order_approved_at",

        "order_delivered_carrier_date",

        "order_delivered_customer_date",

        "order_estimated_delivery_date"

    },

    "order_items": {

        "order_id",

        "order_item_id",

        "product_id",

        "seller_id",

        "shipping_limit_date",

        "price",

        "freight_value"

    },

    "payments": {

        "order_id",

        "payment_sequential",

        "payment_type",

        "payment_installments",

        "payment_value"

    },

    "reviews": {

        "review_id",

        "order_id",

        "review_score",

        "review_comment_title",

        "review_comment_message",

        "review_creation_date",

        "review_answer_timestamp"

    },

    "products": {

        "product_id",

        "product_category_name",

        "product_name_lenght",

        "product_description_lenght",

        "product_photos_qty",

        "product_weight_g",

        "product_length_cm",

        "product_height_cm",

        "product_width_cm"

    },

    "sellers": {

        "seller_id",

        "seller_zip_code_prefix",

        "seller_city",

        "seller_state"

    },

    "geolocation": {

        "geolocation_zip_code_prefix",

        "geolocation_lat",

        "geolocation_lng",

        "geolocation_city",

        "geolocation_state"

    },

    "category_translation": {

        "product_category_name",

        "product_category_name_english"

    }

}

MIN_RECORD_COUNTS = {

    "customers": 1000,

    "orders": 1000,

    "order_items": 1000,

    "payments": 1000,

    "reviews": 1000,

    "products": 100,

    "sellers": 100,

    "geolocation": 1000,

    "category_translation": 10

}

# ---------------------------------------------------------

# Helper

# ---------------------------------------------------------

def get_dataset_name(key):

    """

    Bronze/orders/olist_orders_dataset.csv

    returns:

        orders

    """

    parts = key.split("/")

    if len(parts) < 3:

        raise ValueError(

            f"Invalid Bronze S3 path: {key}"

        )

    return parts[1]

# ---------------------------------------------------------

# Validate CSV

# ---------------------------------------------------------

def validate_csv(bucket, key, dataset_name):

    logger.info(

        f"Starting validation for {bucket}/{key}"

    )

    # -----------------------------------------------------

    # 1. Check object exists

    # -----------------------------------------------------

    try:

        response = s3.head_object(

            Bucket=bucket,

            Key=key

        )

    except Exception as e:

        raise Exception(

            f"File does not exist: s3://{bucket}/{key}"

        ) from e

    # -----------------------------------------------------

    # 2. Check file size

    # -----------------------------------------------------

    file_size = response.get("ContentLength", 0)

    logger.info(

        f"File size: {file_size} bytes"

    )

    if file_size == 0:

        raise ValueError(

            "File is empty"

        )

    # -----------------------------------------------------

    # 3. Check extension

    # -----------------------------------------------------

    if not key.lower().endswith(".csv"):

        raise ValueError(

            "File format is not CSV"

        )

    # -----------------------------------------------------

    # Download object

    # -----------------------------------------------------

    obj = s3.get_object(

        Bucket=bucket,

        Key=key

    )

    body = obj["Body"].read()

    # -----------------------------------------------------

    # Decode

    # -----------------------------------------------------

    try:

        text = body.decode("utf-8-sig")

    except UnicodeDecodeError as e:

        raise ValueError(

            "File is not valid UTF-8 CSV"

        ) from e

    # -----------------------------------------------------

    # Parse CSV

    # -----------------------------------------------------

    reader = csv.DictReader(

        io.StringIO(text)

    )

    actual_columns = set(

        reader.fieldnames or []

    )

    expected_columns = EXPECTED_SCHEMAS[

        dataset_name

    ]

    # -----------------------------------------------------

    # 4. Schema validation

    # -----------------------------------------------------

    missing_columns = (

        expected_columns - actual_columns

    )

    if missing_columns:

        raise ValueError(

            f"Missing columns: "

            f"{sorted(missing_columns)}"

        )

    # -----------------------------------------------------

    # Record count

    # -----------------------------------------------------

    #record_count = 0

    #for row in reader:

    #    record_count += 1

    #logger.info(

    #    f"Record count: {record_count}"

    #)

    # -----------------------------------------------------

    # 5. Record count validation

    # -----------------------------------------------------

    #minimum_count = MIN_RECORD_COUNTS[

    #    dataset_name

    #]

    #if record_count < minimum_count:

    #    raise ValueError(

    #        f"Record count {record_count} "

    #        f"is below minimum "

    #        f"threshold {minimum_count}"

    #    )

    # -----------------------------------------------------

    # Validation successful

    # -----------------------------------------------------

    result = {

        "status": "PASS",

        "bucket": bucket,

        "key": key,

        "dataset": dataset_name,

        "file_size_bytes": file_size,

        #"record_count": record_count,

        "column_count": len(actual_columns),

        "columns": sorted(actual_columns)

    }

    logger.info(

        f"VALIDATION PASSED: {result}"

    )

    return result

# ---------------------------------------------------------

# Lambda Handler

# ---------------------------------------------------------

def lambda_handler(event, context):

    logger.info(

        f"Received event: {json.dumps(event)}"

    )

    try:

        # -------------------------------------------------

        # Extract S3 event

        # -------------------------------------------------

        record = event["Records"][0]

        bucket = record["s3"]["bucket"]["name"]

        key = record["s3"]["object"]["key"]

        # S3 event keys may be URL encoded

        from urllib.parse import unquote_plus

        key = unquote_plus(key)

        # -------------------------------------------------

        # Ignore non-Bronze files

        # -------------------------------------------------

        if not key.startswith("Bronze/"):

            logger.info(

                f"Ignoring non-Bronze object: {key}"

            )

            return {

                "statusCode": 200,

                "message": "Ignored non-Bronze object"

            }

        # -------------------------------------------------

        # Determine dataset

        # -------------------------------------------------

        dataset_name = get_dataset_name(key)

        if dataset_name not in EXPECTED_SCHEMAS:

            raise ValueError(

                f"Unknown dataset: {dataset_name}"

            )

        # -------------------------------------------------

        # Validate

        # -------------------------------------------------

        result = validate_csv(

            bucket,

            key,

            dataset_name

        )

        return {

            "statusCode": 200,

            "validation": result

        }

    except Exception as e:

        logger.exception(

            "Bronze validation FAILED"

        )

        return {

            "statusCode": 400,

            "status": "FAIL",

            "error": str(e)

        }
