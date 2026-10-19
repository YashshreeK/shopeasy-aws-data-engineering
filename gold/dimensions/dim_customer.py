import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

SILVER_DATABASE = "shopeasy_silver_dev"
CUSTOMERS_TABLE = "customers"
ORDERS_TABLE = "orders"

# Replace the placeholder with your S3 bucket when deploying.
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/dim_customer/"

# Read Silver customers and orders from the Glue Data Catalog.
customers_df = glue_context.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=CUSTOMERS_TABLE,
    transformation_ctx="silver_customers_source",
).toDF()

orders_df = glue_context.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=ORDERS_TABLE,
    transformation_ctx="silver_orders_source",
).toDF()

customers_df = customers_df.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
    "dq_customer_valid",
)

orders_df = orders_df.select(
    "order_id", "customer_id", "order_purchase_timestamp"
)

# Grain: one row per customer_unique_id with at least one matching order.
customer_orders_df = customers_df.join(orders_df, on="customer_id", how="inner")

customer_metrics_df = customer_orders_df.groupBy("customer_unique_id").agg(
    F.min("order_purchase_timestamp").alias("first_order_date"),
    F.max("order_purchase_timestamp").alias("last_order_date"),
    F.countDistinct("order_id").alias("total_orders"),
)

# Keep the location associated with each customer's most recent order.
latest_location_window = Window.partitionBy("customer_unique_id").orderBy(
    F.col("order_purchase_timestamp").desc(),
    F.col("customer_id").desc(),
)

latest_customer_df = (
    customer_orders_df
    .withColumn("location_rank", F.row_number().over(latest_location_window))
    .filter(F.col("location_rank") == 1)
    .select(
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "dq_customer_valid",
    )
)

dim_customer_df = latest_customer_df.join(
    customer_metrics_df, on="customer_unique_id", how="inner"
)

# Stable hash key across reloads (not a sequential integer surrogate key).
dim_customer_df = dim_customer_df.withColumn(
    "customer_key", F.xxhash64(F.col("customer_unique_id"))
)

dim_customer_df = dim_customer_df.withColumn(
    "dq_customer_gold_valid",
    (
        F.col("customer_unique_id").isNotNull()
        & F.col("customer_key").isNotNull()
        & F.col("customer_zip_code_prefix").isNotNull()
        & F.col("customer_city").isNotNull()
        & F.col("customer_state").isNotNull()
        & F.col("first_order_date").isNotNull()
        & F.col("last_order_date").isNotNull()
        & (F.col("total_orders") > 0)
        & F.col("dq_customer_valid")
    ),
)

dim_customer_df = (
    dim_customer_df
    .withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Silver"))
    .withColumn("source_dataset", F.lit("customers,orders"))
)

dim_customer_df = dim_customer_df.select(
    "customer_key",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
    "first_order_date",
    "last_order_date",
    "total_orders",
    "dq_customer_valid",
    "dq_customer_gold_valid",
    "gold_processed_at",
    "source_layer",
    "source_dataset",
)

(
    dim_customer_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)

job.commit()
