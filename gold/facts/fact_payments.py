"""ShopEasy Gold payment fact ETL (AWS Glue / PySpark).

Grain: one payment per (order_id, payment_sequential), provided lookup keys
are unique. The payment_date_key uses the order purchase date because the
source payments dataset does not provide a separate payment timestamp.

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

SILVER_DATABASE = "shopeasy_silver_dev"
GOLD_DATABASE = "shopeasy_gold_dev"
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/fact_payments/"

# 1. Read Silver source tables.
payments_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name="payments",
        transformation_ctx="silver_payments_source",
    ).toDF()
)
orders_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name="orders",
        transformation_ctx="silver_orders_source",
    ).toDF()
)
customers_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=SILVER_DATABASE,
        table_name="customers",
        transformation_ctx="silver_customers_source",
    ).toDF()
)

# 2. Read Gold dimension tables.
dim_customer_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="dim_customer",
        transformation_ctx="gold_customer_source",
    ).toDF()
)
dim_date_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="dim_date",
        transformation_ctx="gold_date_source",
    ).toDF()
)

# 3. Select required columns.
payments_df = payments_df.select(
    "order_id", "payment_sequential", "payment_type",
    "payment_installments", "payment_value", "dq_payment_valid"
)
orders_df = orders_df.select(
    "order_id", "customer_id", "order_status", "order_purchase_timestamp"
)
customers_df = customers_df.select("customer_id", "customer_unique_id")
customer_dimension_df = dim_customer_df.select("customer_key", "customer_unique_id")
date_dimension_df = dim_date_df.select("date_key", "full_date")

# 4. Resolve customer through orders and customers.
fact_df = payments_df.join(orders_df, on="order_id", how="left")
fact_df = fact_df.join(customers_df, on="customer_id", how="left")
fact_df = fact_df.join(
    customer_dimension_df, on="customer_unique_id", how="left"
)

# 5. Resolve date dimension key from the order purchase date.
fact_df = fact_df.withColumn(
    "payment_date", F.to_date("order_purchase_timestamp")
)
fact_df = fact_df.join(
    date_dimension_df,
    fact_df["payment_date"] == date_dimension_df["full_date"],
    how="left",
)
fact_df = fact_df.withColumnRenamed("date_key", "payment_date_key")
fact_df = fact_df.drop("full_date")

# 6. Add Gold data quality flag; invalid records remain in the output.
fact_df = fact_df.withColumn(
    "dq_fact_payment_valid",
    (
        F.col("order_id").isNotNull()
        & F.col("payment_sequential").isNotNull()
        & F.col("customer_key").isNotNull()
        & F.col("payment_date_key").isNotNull()
        & F.col("payment_type").isNotNull()
        & F.col("payment_value").isNotNull()
        & F.col("dq_payment_valid")
    ),
)

# 7. Audit and lineage.
fact_df = (
    fact_df.withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Silver"))
    .withColumn("source_dataset", F.lit("payments,orders,customers"))
)

# 8. Final schema.
fact_payments_df = fact_df.select(
    "order_id",
    "payment_sequential",
    "customer_key",
    "payment_date_key",
    "payment_type",
    "payment_installments",
    "payment_value",
    "order_status",
    "order_purchase_timestamp",
    "dq_payment_valid",
    "dq_fact_payment_valid",
    "gold_processed_at",
    "source_layer",
    "source_dataset",
)

# 9. Write Gold Parquet; each run fully overwrites the target.
(
    fact_payments_df.write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)

job.commit()
