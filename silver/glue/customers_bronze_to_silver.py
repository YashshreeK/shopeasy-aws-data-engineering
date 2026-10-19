"""ShopEasy Bronze-to-Silver customers ETL (AWS Glue / PySpark).

Configure SILVER_PATH for your own S3 bucket before running.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# Job initialization
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Configuration
BRONZE_DATABASE = "shopeasy_bronze_dev"
BRONZE_TABLE = "customers"
SILVER_PATH = "s3://YOUR_SHOPEASY_BUCKET/Silver/customers/"

# Read Bronze customers from Glue Data Catalog
bronze_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_customers_source",
)
df = bronze_dyf.toDF()

# Normalize / clean string columns
string_columns = [
    "customer_id",
    "customer_unique_id",
    "customer_city",
    "customer_state",
]
for column_name in string_columns:
    df = df.withColumn(
        column_name,
        F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(
            F.trim(F.col(column_name))
        ),
    )

# Normalize ZIP prefix
df = df.withColumn(
    "customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("string")
)

# Standardize city/state
df = df.withColumn("customer_city", F.lower(F.trim(F.col("customer_city"))))
df = df.withColumn("customer_state", F.upper(F.trim(F.col("customer_state"))))

# Deduplicate on customer_id
df = df.dropDuplicates(["customer_id"])

# Data-quality rules
missing_customer_id = F.col("customer_id").isNull()
missing_unique_id = F.col("customer_unique_id").isNull()
missing_location = (
    F.col("customer_city").isNull()
    | F.col("customer_state").isNull()
    | F.col("customer_zip_code_prefix").isNull()
)

# Add DQ fields
df = df.withColumn(
    "dq_customer_valid", ~(missing_customer_id | missing_unique_id | missing_location)
)
df = df.withColumn(
    "dq_issue_code",
    F.when(missing_customer_id, F.lit("MISSING_CUSTOMER_ID"))
    .when(missing_unique_id, F.lit("MISSING_CUSTOMER_UNIQUE_ID"))
    .when(missing_location, F.lit("MISSING_CUSTOMER_LOCATION"))
    .otherwise(F.lit(None)),
)

# Add audit / lineage columns
df = df.withColumn("silver_processed_at", F.current_timestamp())
df = df.withColumn("source_layer", F.lit("Bronze"))
df = df.withColumn("source_dataset", F.lit("customers"))

# Write Silver as Parquet
(
    df.write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(SILVER_PATH)
)

job.commit()
