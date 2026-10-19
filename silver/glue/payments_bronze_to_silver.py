"""ShopEasy payments Bronze-to-Silver AWS Glue ETL.

Set YOUR_SHOPEASY_BUCKET to your S3 bucket before deployment.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BRONZE_DATABASE = "shopeasy_bronze_dev"
BRONZE_TABLE = "payments"
SILVER_PATH = "s3://YOUR_SHOPEASY_BUCKET/Silver/payments/"

# 1. Read Bronze payments via Glue Data Catalog.
bronze_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_payments_source",
)
df = bronze_dyf.toDF()

# 2. Clean strings.
string_columns = ["order_id", "payment_type"]
for column_name in string_columns:
    df = df.withColumn(
        column_name,
        F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(
            F.trim(F.col(column_name))
        ),
    )

# 3. Standardize payment type.
df = df.withColumn("payment_type", F.lower(F.col("payment_type")))

# 4. Cast numeric columns.
df = df.withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
df = df.withColumn("payment_installments", F.col("payment_installments").cast("int"))
df = df.withColumn("payment_value", F.col("payment_value").cast("double"))

# 5. Deduplicate using payment business key.
df = df.dropDuplicates(["order_id", "payment_sequential"])

# 6. Data-quality rules.
missing_order_id = F.col("order_id").isNull()
invalid_payment_sequence = F.col("payment_sequential").isNull() | (
    F.col("payment_sequential") <= 0
)
missing_payment_type = F.col("payment_type").isNull()
invalid_installments = F.col("payment_installments").isNull() | (
    F.col("payment_installments") < 0
)
invalid_payment_value = F.col("payment_value").isNull() | (
    F.col("payment_value") < 0
)

# 7. Add DQ fields.
df = df.withColumn(
    "dq_payment_valid",
    ~(
        missing_order_id
        | invalid_payment_sequence
        | missing_payment_type
        | invalid_installments
        | invalid_payment_value
    ),
)
df = df.withColumn(
    "dq_issue_code",
    F.when(missing_order_id, F.lit("MISSING_ORDER_ID"))
    .when(invalid_payment_sequence, F.lit("INVALID_PAYMENT_SEQUENCE"))
    .when(missing_payment_type, F.lit("MISSING_PAYMENT_TYPE"))
    .when(invalid_installments, F.lit("INVALID_INSTALLMENTS"))
    .when(invalid_payment_value, F.lit("INVALID_PAYMENT_VALUE"))
    .otherwise(F.lit(None)),
)

# 8. Add audit / lineage fields.
df = df.withColumn("silver_processed_at", F.current_timestamp())
df = df.withColumn("source_layer", F.lit("Bronze"))
df = df.withColumn("source_dataset", F.lit("payments"))

# 9. Write Silver Parquet.
(
    df.write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(SILVER_PATH)
)

job.commit()
