import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


# =========================================================
# 1. Initialize Glue job
# =========================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


SILVER_DATABASE = "shopeasy_silver_dev"
GOLD_DATABASE = "shopeasy_gold_dev"

GOLD_PATH = (
    "s3://YOUR_SHOPEASY_BUCKET/"
    "Gold/fact_order_items/"
)


# =========================================================
# 2. Read Silver order_items
#
# This defines the grain of the fact table.
# Expected rows = 112,650
# =========================================================

order_items_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name="order_items",
        transformation_ctx="silver_order_items_source"
    )
    .toDF()
)


# =========================================================
# 3. Read Silver orders
# =========================================================

orders_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name="orders",
        transformation_ctx="silver_orders_source"
    )
    .toDF()
)


# =========================================================
# 4. Read Silver customers
#
# Required because:
#
# orders.customer_id
#       ->
# customers.customer_unique_id
#       ->
# dim_customer.customer_key
# =========================================================

customers_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name="customers",
        transformation_ctx="silver_customers_source"
    )
    .toDF()
)


# =========================================================
# 5. Read Gold dimensions
# =========================================================

dim_customer_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="dim_customer",
        transformation_ctx="gold_customer_source"
    )
    .toDF()
)


dim_product_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="dim_product",
        transformation_ctx="gold_product_source"
    )
    .toDF()
)


dim_seller_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="dim_seller",
        transformation_ctx="gold_seller_source"
    )
    .toDF()
)


dim_location_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="dim_location",
        transformation_ctx="gold_location_source"
    )
    .toDF()
)


dim_date_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="dim_date",
        transformation_ctx="gold_date_source"
    )
    .toDF()
)


# =========================================================
# 6. Select only columns required from each source
# =========================================================

order_items_df = order_items_df.select(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
    "item_total_value",
    "dq_order_item_valid"
)


orders_df = orders_df.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "dq_date_sequence_valid",
    "dq_issue_code",
    "is_late_delivery"
)


customers_df = customers_df.select(
    "customer_id",
    "customer_unique_id"
)


customer_dimension_df = dim_customer_df.select(
    "customer_key",
    "customer_unique_id",
    "customer_zip_code_prefix"
)


product_dimension_df = dim_product_df.select(
    "product_key",
    "product_id"
)


seller_dimension_df = dim_seller_df.select(
    "seller_key",
    "seller_id",
    F.col("location_key").alias("seller_location_key")
)


# Exclude the special unknown location from normal ZIP lookup
location_lookup_df = (
    dim_location_df
    .filter(F.col("location_key") != -1)
    .select(
        F.col("location_key").alias("customer_location_key"),
        F.col("zip_code_prefix").alias("location_zip_code_prefix")
    )
)


date_dimension_df = dim_date_df.select(
    "date_key",
    "full_date"
)


# =========================================================
# 7. Join order_items -> orders
#
# order_items remains the fact grain.
# =========================================================

fact_df = order_items_df.join(
    orders_df,
    on="order_id",
    how="left"
)


# =========================================================
# 8. Resolve customer_id -> customer_unique_id
# =========================================================

fact_df = fact_df.join(
    customers_df,
    on="customer_id",
    how="left"
)


# =========================================================
# 9. Resolve customer_unique_id -> customer_key
#
# We also obtain the customer's latest/current ZIP from
# dim_customer.
# =========================================================

fact_df = fact_df.join(
    customer_dimension_df,
    on="customer_unique_id",
    how="left"
)


# =========================================================
# 10. Resolve customer ZIP -> customer location
#
# Missing geolocation reference becomes -1.
# =========================================================

fact_df = fact_df.join(
    location_lookup_df,
    fact_df["customer_zip_code_prefix"]
    == location_lookup_df["location_zip_code_prefix"],
    how="left"
)

fact_df = fact_df.withColumn(
    "customer_location_key",
    F.coalesce(
        F.col("customer_location_key"),
        F.lit(-1).cast("long")
    )
)

fact_df = fact_df.drop(
    "location_zip_code_prefix"
)


# =========================================================
# 11. Resolve product_id -> product_key
# =========================================================

fact_df = fact_df.join(
    product_dimension_df,
    on="product_id",
    how="left"
)


# =========================================================
# 12. Resolve seller_id -> seller_key
#
# dim_seller already contains seller_location_key.
# =========================================================

fact_df = fact_df.join(
    seller_dimension_df,
    on="seller_id",
    how="left"
)


# =========================================================
# 13. Generate transaction date
# =========================================================

fact_df = fact_df.withColumn(
    "order_date",
    F.to_date("order_purchase_timestamp")
)


# =========================================================
# 14. Resolve order date -> date dimension key
#
# We perform the actual dimension lookup rather than simply
# calculating yyyyMMdd. This validates referential integrity.
# =========================================================

fact_df = fact_df.join(
    date_dimension_df,
    fact_df["order_date"]
    == date_dimension_df["full_date"],
    how="left"
)

fact_df = fact_df.withColumnRenamed(
    "date_key",
    "order_date_key"
)

fact_df = fact_df.drop(
    "full_date"
)


# =========================================================
# 15. Unknown foreign-key handling
#
# Location already has an Unknown member (-1).
#
# For customer/product/seller/date we do NOT silently
# replace missing keys with -1 because those dimensions
# currently do not have Unknown members.
#
# Missing keys should instead fail Gold DQ so the issue
# becomes visible.
# =========================================================


# =========================================================
# 16. Create fact-level DQ flag
# =========================================================

fact_df = fact_df.withColumn(
    "dq_fact_order_item_valid",
    (
        F.col("order_id").isNotNull()
        & F.col("order_item_id").isNotNull()

        & F.col("customer_key").isNotNull()
        & F.col("product_key").isNotNull()
        & F.col("seller_key").isNotNull()
        & F.col("order_date_key").isNotNull()

        & F.col("customer_location_key").isNotNull()
        & F.col("seller_location_key").isNotNull()

        & F.col("price").isNotNull()
        & F.col("freight_value").isNotNull()
        & F.col("item_total_value").isNotNull()

        & F.col("dq_order_item_valid")
    )
)


# =========================================================
# 17. Add audit / lineage
# =========================================================

fact_df = (
    fact_df
    .withColumn(
        "gold_processed_at",
        F.current_timestamp()
    )
    .withColumn(
        "source_layer",
        F.lit("Silver")
    )
    .withColumn(
        "source_dataset",
        F.lit("order_items,orders,customers")
    )
)


# =========================================================
# 18. Final fact schema
# =========================================================

fact_order_items_df = fact_df.select(

    # Natural / transaction identifiers
    "order_id",
    "order_item_id",

    # Dimension foreign keys
    "customer_key",
    "product_key",
    "seller_key",
    "order_date_key",

    "customer_location_key",
    "seller_location_key",

    # Transaction attributes
    "order_status",

    "order_purchase_timestamp",
    "order_approved_at",
    "shipping_limit_date",

    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",

    # Measures
    "price",
    "freight_value",
    "item_total_value",

    # Business indicators
    "is_late_delivery",

    # Source DQ
    "dq_order_item_valid",
    "dq_date_sequence_valid",
    "dq_issue_code",

    # Gold DQ
    "dq_fact_order_item_valid",

    # Audit
    "gold_processed_at",
    "source_layer",
    "source_dataset"
)


# =========================================================
# 19. Write Gold fact table
# =========================================================

(
    fact_order_items_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)


job.commit()
