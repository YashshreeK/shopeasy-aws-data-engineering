"""ShopEasy Gold monthly sales mart.

Requires Gold fact_order_items and dim_date in the Glue Data Catalog.
Replace YOUR_SHOPEASY_BUCKET with your S3 bucket before running.
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

GOLD_DATABASE = "shopeasy_gold_dev"
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/mart_monthly_sales/"

# Read Gold fact and date dimension.
fact_df = glue_context.create_dynamic_frame.from_catalog(
    database=GOLD_DATABASE,
    table_name="fact_order_items",
    transformation_ctx="fact_order_items_source",
).toDF()

date_df = glue_context.create_dynamic_frame.from_catalog(
    database=GOLD_DATABASE,
    table_name="dim_date",
    transformation_ctx="dim_date_source",
).toDF()

# Join on the purchase-date key; unmatched facts are excluded by the inner join.
date_df = date_df.select(
    F.col("date_key").alias("order_date_key"),
    "year",
    "month",
    "month_name",
)
sales_df = fact_df.join(date_df, on="order_date_key", how="inner")

# Aggregate at calendar-month grain.
monthly_df = sales_df.groupBy("year", "month", "month_name").agg(
    F.countDistinct("order_id").alias("total_orders"),
    F.count("*").alias("total_order_items"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.sum("price").alias("gross_item_revenue"),
    F.sum("freight_value").alias("total_freight"),
    F.sum("item_total_value").alias("total_order_value"),
    F.countDistinct(
        F.when(F.col("is_late_delivery") == True, F.col("order_id"))
    ).alias("late_orders"),
)

monthly_df = (
    monthly_df
    .withColumn(
        "year_month",
        F.format_string("%04d-%02d", F.col("year"), F.col("month")),
    )
    .withColumn(
        "avg_order_value",
        F.round(F.col("total_order_value") / F.col("total_orders"), 2),
    )
    .withColumn(
        "avg_items_per_order",
        F.round(F.col("total_order_items") / F.col("total_orders"), 2),
    )
    .withColumn(
        "late_order_pct",
        F.round(F.col("late_orders") / F.col("total_orders") * 100, 2),
    )
    .withColumn("gross_item_revenue", F.round("gross_item_revenue", 2))
    .withColumn("total_freight", F.round("total_freight", 2))
    .withColumn("total_order_value", F.round("total_order_value", 2))
    .withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Gold"))
    .withColumn("source_dataset", F.lit("fact_order_items,dim_date"))
)

monthly_df = monthly_df.select(
    "year", "month", "month_name", "year_month",
    "total_orders", "total_order_items", "unique_customers",
    "gross_item_revenue", "total_freight", "total_order_value",
    "avg_order_value", "avg_items_per_order",
    "late_orders", "late_order_pct",
    "gold_processed_at", "source_layer", "source_dataset",
)

monthly_df.write.mode("overwrite").format("parquet").option(
    "compression", "snappy"
).save(GOLD_PATH)

job.commit()
