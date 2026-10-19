"""ShopEasy Gold fact_reviews ETL.

Grain: one row per Silver review record, provided lookup keys are unique.
Replace YOUR_SHOPEASY_BUCKET with your S3 bucket when deploying.
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

SILVER_DATABASE = "shopeasy_silver_dev"
GOLD_DATABASE = "shopeasy_gold_dev"
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/fact_reviews/"


def read_table(database, table, context):
    return glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        transformation_ctx=context,
    ).toDF()


# Silver sources
reviews_df = read_table(SILVER_DATABASE, "reviews", "silver_reviews_source").select(
    "review_id", "order_id", "review_score", "review_comment_title",
    "review_comment_message", "review_creation_date", "review_answer_timestamp",
    "has_review_title", "has_review_comment", "dq_review_valid",
)
orders_df = read_table(SILVER_DATABASE, "orders", "silver_orders_source").select(
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
)
customers_df = read_table(SILVER_DATABASE, "customers", "silver_customers_source").select(
    "customer_id", "customer_unique_id",
)

# Gold dimension lookups
customer_dimension_df = read_table(
    GOLD_DATABASE, "dim_customer", "gold_customer_source"
).select("customer_key", "customer_unique_id")
date_dimension_df = read_table(
    GOLD_DATABASE, "dim_date", "gold_date_source"
).select("date_key", "full_date")

# Preserve review records with left joins. Dimension lookup keys must be unique
# to prevent multiplication of source review rows.
fact_df = (
    reviews_df
    .join(orders_df, on="order_id", how="left")
    .join(customers_df, on="customer_id", how="left")
    .join(customer_dimension_df, on="customer_unique_id", how="left")
    .withColumn("review_date", F.to_date("review_creation_date"))
)
fact_df = fact_df.join(
    date_dimension_df,
    fact_df["review_date"] == date_dimension_df["full_date"],
    how="left",
).withColumnRenamed("date_key", "review_date_key").drop("full_date")

# Review response duration in hours; null when either timestamp is missing.
fact_df = fact_df.withColumn(
    "review_response_hours",
    (
        F.unix_timestamp("review_answer_timestamp")
        - F.unix_timestamp("review_creation_date")
    ) / F.lit(3600.0),
)

# Flag invalid records rather than dropping them.
fact_df = fact_df.withColumn(
    "dq_fact_review_valid",
    F.col("review_id").isNotNull()
    & F.col("order_id").isNotNull()
    & F.col("customer_key").isNotNull()
    & F.col("review_date_key").isNotNull()
    & F.col("review_score").isNotNull()
    & F.col("dq_review_valid"),
)

fact_df = (
    fact_df
    .withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Silver"))
    .withColumn("source_dataset", F.lit("reviews,orders,customers"))
)

fact_reviews_df = fact_df.select(
    "review_id", "order_id", "customer_key", "review_date_key",
    "review_score", "review_comment_title", "review_comment_message",
    "has_review_title", "has_review_comment", "review_creation_date",
    "review_answer_timestamp", "review_response_hours", "order_status",
    "order_purchase_timestamp", "dq_review_valid", "dq_fact_review_valid",
    "gold_processed_at", "source_layer", "source_dataset",
)

(
    fact_reviews_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)

job.commit()
