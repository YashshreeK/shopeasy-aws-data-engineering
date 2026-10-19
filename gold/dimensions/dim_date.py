"""Build ShopEasy Gold date dimension from Silver orders lifecycle timestamps."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

SILVER_DATABASE = "shopeasy_silver_dev"
ORDERS_TABLE = "orders"
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/dim_date/"

# Read Silver orders from Glue Data Catalog.
orders_df = glue_context.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=ORDERS_TABLE,
    transformation_ctx="silver_orders_source",
).toDF()

# Gather all five order lifecycle dates (including nullable timestamps).
date_values_df = (
    orders_df.select(
        F.explode(
            F.array(
                F.to_date("order_purchase_timestamp"),
                F.to_date("order_approved_at"),
                F.to_date("order_delivered_carrier_date"),
                F.to_date("order_delivered_customer_date"),
                F.to_date("order_estimated_delivery_date"),
            )
        ).alias("event_date")
    )
    .filter(F.col("event_date").isNotNull())
)

# Produce a continuous calendar covering the full range of lifecycle dates.
date_range_df = date_values_df.agg(
    F.min("event_date").alias("min_date"),
    F.max("event_date").alias("max_date"),
)
calendar_df = date_range_df.select(
    F.explode(
        F.sequence(F.col("min_date"), F.col("max_date"), F.expr("INTERVAL 1 DAY"))
    ).alias("full_date")
)

dim_date_df = (
    calendar_df
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
    .withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Silver"))
    .withColumn("source_dataset", F.lit("orders"))
    .select(
        "date_key", "full_date", "year", "quarter", "month", "month_name",
        "day_of_month", "day_of_week", "day_name", "week_of_year",
        "is_weekend", "gold_processed_at", "source_layer", "source_dataset",
    )
)

(
    dim_date_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)

job.commit()
