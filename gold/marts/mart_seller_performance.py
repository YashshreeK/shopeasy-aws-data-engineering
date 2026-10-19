"""ShopEasy Gold seller performance mart (AWS Glue / PySpark).

Grain: one row per seller in dim_seller.
Replace YOUR_SHOPEASY_BUCKET with your own S3 bucket before running.
Requires Gold fact_order_items and dim_seller in the Glue Data Catalog.
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
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/mart_seller_performance/"

# Load Gold fact and dimension data.
fact_df = glue_context.create_dynamic_frame.from_catalog(
    database=GOLD_DATABASE,
    table_name="fact_order_items",
    transformation_ctx="fact_order_items_source",
).toDF()

seller_df = glue_context.create_dynamic_frame.from_catalog(
    database=GOLD_DATABASE,
    table_name="dim_seller",
    transformation_ctx="dim_seller_source",
).toDF()

fact_df = fact_df.select(
    "order_id", "seller_key", "customer_key", "product_key", "price",
    "freight_value", "item_total_value", "is_late_delivery",
)
seller_df = seller_df.select(
    "seller_key", "seller_id", "seller_zip_code_prefix", "seller_city",
    "seller_state", "location_key", "has_resolved_location",
    "dq_seller_gold_valid",
)

# Aggregate at seller grain. Distinct order counts avoid item-level inflation.
seller_metrics_df = fact_df.groupBy("seller_key").agg(
    F.count("*").alias("total_items"),
    F.countDistinct("order_id").alias("total_orders"),
    F.countDistinct("customer_key").alias("unique_customers"),
    F.countDistinct("product_key").alias("unique_products"),
    F.sum("price").alias("gross_item_revenue"),
    F.sum("freight_value").alias("total_freight"),
    F.sum("item_total_value").alias("total_order_value"),
    F.countDistinct(
        F.when(F.col("is_late_delivery") == True, F.col("order_id"))
    ).alias("late_orders"),
)

# Preserve sellers with no fact rows.
mart_df = seller_df.join(seller_metrics_df, on="seller_key", how="left")
for column in (
    "total_items", "total_orders", "unique_customers", "unique_products", "late_orders"
):
    mart_df = mart_df.withColumn(column, F.coalesce(F.col(column), F.lit(0)))
for column in ("gross_item_revenue", "total_freight", "total_order_value"):
    mart_df = mart_df.withColumn(column, F.coalesce(F.col(column), F.lit(0.0)))

# Derived KPIs with zero-denominator handling.
mart_df = (
    mart_df
    .withColumn(
        "avg_item_value",
        F.when(F.col("total_items") > 0,
               F.round(F.col("total_order_value") / F.col("total_items"), 2))
         .otherwise(F.lit(0.0)),
    )
    .withColumn(
        "avg_order_value",
        F.when(F.col("total_orders") > 0,
               F.round(F.col("total_order_value") / F.col("total_orders"), 2))
         .otherwise(F.lit(0.0)),
    )
    .withColumn(
        "avg_items_per_order",
        F.when(F.col("total_orders") > 0,
               F.round(F.col("total_items") / F.col("total_orders"), 2))
         .otherwise(F.lit(0.0)),
    )
    .withColumn(
        "freight_pct_of_value",
        F.when(F.col("total_order_value") > 0,
               F.round(F.col("total_freight") / F.col("total_order_value") * 100, 2))
         .otherwise(F.lit(0.0)),
    )
    .withColumn(
        "late_order_pct",
        F.when(F.col("total_orders") > 0,
               F.round(F.col("late_orders") / F.col("total_orders") * 100, 2))
         .otherwise(F.lit(0.0)),
    )
)

# The seller_key tie-breaker produces a unique deterministic ranking.
revenue_rank_window = Window.orderBy(
    F.col("total_order_value").desc(), F.col("seller_key").asc()
)
mart_df = mart_df.withColumn("revenue_rank", F.dense_rank().over(revenue_rank_window))

overall_window = Window.partitionBy()
mart_df = mart_df.withColumn(
    "revenue_contribution_pct",
    F.when(
        F.sum("total_order_value").over(overall_window) > 0,
        F.round(
            F.col("total_order_value") /
            F.sum("total_order_value").over(overall_window) * 100, 4
        ),
    ).otherwise(F.lit(0.0)),
)

mart_df = mart_df.withColumn(
    "seller_performance_segment",
    F.when(
        (F.col("revenue_rank") <= 100) & (F.col("late_order_pct") <= 10),
        F.lit("Top Performer"),
    )
    .when(F.col("revenue_rank") <= 100, F.lit("High Revenue - Delivery Risk"))
    .when(F.col("late_order_pct") > 20, F.lit("Delivery Risk"))
    .otherwise(F.lit("Standard")),
)

for column in ("gross_item_revenue", "total_freight", "total_order_value"):
    mart_df = mart_df.withColumn(column, F.round(F.col(column), 2))

mart_df = mart_df.withColumn(
    "dq_seller_performance_valid",
    F.col("seller_key").isNotNull()
    & F.col("seller_id").isNotNull()
    & (F.col("total_orders") >= 0)
    & (F.col("total_order_value") >= 0)
    & F.col("dq_seller_gold_valid"),
)
mart_df = (
    mart_df.withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Gold"))
    .withColumn("source_dataset", F.lit("dim_seller,fact_order_items"))
)

mart_seller_performance_df = mart_df.select(
    "seller_key", "seller_id", "seller_zip_code_prefix", "seller_city",
    "seller_state", "location_key", "has_resolved_location",
    "total_orders", "total_items", "unique_customers", "unique_products",
    "gross_item_revenue", "total_freight", "total_order_value",
    "avg_item_value", "avg_order_value", "avg_items_per_order",
    "freight_pct_of_value", "late_orders", "late_order_pct",
    "revenue_rank", "revenue_contribution_pct", "seller_performance_segment",
    "dq_seller_performance_valid", "gold_processed_at", "source_layer",
    "source_dataset",
)

(
    mart_seller_performance_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)
job.commit()
