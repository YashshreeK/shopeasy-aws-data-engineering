"""ShopEasy order-items Bronze-to-Silver AWS Glue ETL job.

Required Glue argument: --JOB_NAME
Replace YOUR_SHOPEASY_BUCKET with your S3 bucket before deployment.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

BRONZE_DATABASE = "shopeasy_bronze_dev"
BRONZE_TABLE = "order_items"
SILVER_PATH = "s3://YOUR_SHOPEASY_BUCKET/Silver/order_items/"

# Read the Bronze Glue Data Catalog table.
bronze_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_order_items_source",
)
df = bronze_dyf.toDF()

# Normalize blank strings to null and trim whitespace.
string_columns = ["order_id", "product_id", "seller_id", "shipping_limit_date"]
for column_name in string_columns:
    df = df.withColumn(
        column_name,
        F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(
            F.trim(F.col(column_name))
        ),
    )

# Explicit data types.
df = df.withColumn("order_item_id", F.col("order_item_id").cast("int"))
df = df.withColumn("price", F.col("price").cast("double"))
df = df.withColumn("freight_value", F.col("freight_value").cast("double"))
df = df.withColumn(
    "shipping_limit_date", F.col("shipping_limit_date").cast(TimestampType())
)

# Composite key: one order may contain multiple items.
df = df.dropDuplicates(["order_id", "order_item_id"])

# Data-quality conditions.
missing_order_id = F.col("order_id").isNull()
missing_product_id = F.col("product_id").isNull()
missing_seller_id = F.col("seller_id").isNull()
invalid_order_item_id = F.col("order_item_id").isNull() | (F.col("order_item_id") <= 0)
invalid_price = F.col("price").isNull() | (F.col("price") < 0)
invalid_freight = F.col("freight_value").isNull() | (F.col("freight_value") < 0)
missing_shipping_limit = F.col("shipping_limit_date").isNull()

df = df.withColumn(
    "dq_order_item_valid",
    ~(
        missing_order_id
        | missing_product_id
        | missing_seller_id
        | invalid_order_item_id
        | invalid_price
        | invalid_freight
        | missing_shipping_limit
    ),
)

df = df.withColumn(
    "dq_issue_code",
    F.when(missing_order_id, F.lit("MISSING_ORDER_ID"))
    .when(missing_product_id, F.lit("MISSING_PRODUCT_ID"))
    .when(missing_seller_id, F.lit("MISSING_SELLER_ID"))
    .when(invalid_order_item_id, F.lit("INVALID_ORDER_ITEM_ID"))
    .when(invalid_price, F.lit("INVALID_PRICE"))
    .when(invalid_freight, F.lit("INVALID_FREIGHT_VALUE"))
    .when(missing_shipping_limit, F.lit("MISSING_SHIPPING_LIMIT_DATE"))
    .otherwise(F.lit(None)),
)

# Business metric and lineage.
df = df.withColumn("item_total_value", F.col("price") + F.col("freight_value"))
df = df.withColumn("silver_processed_at", F.current_timestamp())
df = df.withColumn("source_layer", F.lit("Bronze"))
df = df.withColumn("source_dataset", F.lit("order_items"))

# Full overwrite of Silver order-items data; invalid rows remain flagged.
(
    df.write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(SILVER_PATH)
)
job.commit()
