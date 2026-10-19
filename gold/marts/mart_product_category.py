"""ShopEasy Gold product-category performance mart (AWS Glue / PySpark).

Requires Glue Catalog tables shopeasy_gold_dev.fact_order_items and dim_product.
Replace YOUR_SHOPEASY_BUCKET with your own bucket before deploying.
"""

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

GOLD_DATABASE = "shopeasy_gold_dev"
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/mart_product_category/"

# Read Gold fact and product dimension.
fact_df = glue_context.create_dynamic_frame.from_catalog(
    database=GOLD_DATABASE,
    table_name="fact_order_items",
    transformation_ctx="fact_order_items_source",
).toDF()

product_df = glue_context.create_dynamic_frame.from_catalog(
    database=GOLD_DATABASE,
    table_name="dim_product",
    transformation_ctx="dim_product_source",
).toDF()

fact_df = fact_df.select(
    "order_id", "product_key", "customer_key", "seller_key",
    "price", "freight_value", "item_total_value", "is_late_delivery",
)
product_df = product_df.select(
    "product_key", "reporting_category_name", "dq_product_gold_valid"
)

# Left join keeps items without a matched product dimension.
category_sales_df = fact_df.join(product_df, on="product_key", how="left")
category_sales_df = category_sales_df.withColumn(
    "reporting_category_name",
    F.coalesce(F.col("reporting_category_name"), F.lit("unknown")),
)

# One output row per reporting category.
category_df = category_sales_df.groupBy("reporting_category_name").agg(
    F.count("*").alias("total_items"),
    F.countDistinct("order_id").alias("total_orders"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.countDistinct("seller_key").alias("unique_sellers"),
    F.sum("price").alias("gross_item_revenue"),
    F.sum("freight_value").alias("total_freight"),
    F.sum("item_total_value").alias("total_order_value"),
    F.countDistinct(
        F.when(F.col("is_late_delivery") == True, F.col("order_id"))
    ).alias("late_orders"),
    F.sum(
        F.when(F.col("dq_product_gold_valid") == False, 1).otherwise(0)
    ).alias("items_from_invalid_products"),
)

category_df = (
    category_df
    .withColumn(
        "avg_item_value",
        F.round(F.col("total_order_value") / F.col("total_items"), 2),
    )
    .withColumn(
        "avg_items_per_order",
        F.round(F.col("total_items") / F.col("total_orders"), 2),
    )
    .withColumn(
        "avg_order_value",
        F.round(F.col("total_order_value") / F.col("total_orders"), 2),
    )
    .withColumn(
        "late_order_pct",
        F.round(F.col("late_orders") / F.col("total_orders") * 100, 2),
    )
)

# Percentage of total category revenue and descending revenue rank.
overall_window = Window.partitionBy()
category_df = category_df.withColumn(
    "revenue_contribution_pct",
    F.round(
        F.col("total_order_value")
        / F.sum("total_order_value").over(overall_window) * 100,
        2,
    ),
)
rank_window = Window.orderBy(F.col("total_order_value").desc())
category_df = category_df.withColumn(
    "revenue_rank", F.dense_rank().over(rank_window)
)

for monetary_col in ("gross_item_revenue", "total_freight", "total_order_value"):
    category_df = category_df.withColumn(monetary_col, F.round(monetary_col, 2))

category_df = (
    category_df
    .withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Gold"))
    .withColumn("source_dataset", F.lit("fact_order_items,dim_product"))
)

category_df = category_df.select(
    "reporting_category_name",
    "total_items", "total_orders", "unique_customers", "unique_sellers",
    "gross_item_revenue", "total_freight", "total_order_value",
    "avg_item_value", "avg_order_value", "avg_items_per_order",
    "late_orders", "late_order_pct", "revenue_contribution_pct", "revenue_rank",
    "items_from_invalid_products", "gold_processed_at", "source_layer",
    "source_dataset",
)

category_df.write.mode("overwrite").format("parquet").option(
    "compression", "snappy"
).save(GOLD_PATH)

job.commit()
