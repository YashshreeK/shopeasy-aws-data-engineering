"""ShopEasy Gold seller dimension ETL (AWS Glue / PySpark).

Prerequisite: shopeasy_gold_dev.dim_location must be populated and
contain a unique mapping for each non-unknown zip_code_prefix.
Replace YOUR_SHOPEASY_BUCKET with your deployment bucket before running.
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
SELLERS_TABLE = "sellers"
LOCATION_TABLE = "dim_location"
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/dim_seller/"

# 1. Read Silver sellers and Gold location dimension.
sellers_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=SELLERS_TABLE,
    transformation_ctx="silver_sellers_source",
)
sellers_df = sellers_dyf.toDF()
location_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=GOLD_DATABASE,
    table_name=LOCATION_TABLE,
    transformation_ctx="gold_location_source",
)
location_df = location_dyf.toDF()

# 2. Select the seller attributes needed for the dimension.
sellers_df = sellers_df.select(
    "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state", "dq_seller_valid"
)

# 3. Exclude the unknown location row from ZIP lookup.
# Assumes one location_key per ZIP prefix in dim_location.
location_lookup_df = (
    location_df.filter(F.col("location_key") != -1)
    .select(
        F.col("zip_code_prefix").alias("location_zip_code_prefix"),
        "location_key",
    )
)

# 4. Resolve ZIP to location key, falling back to the unknown member.
dim_seller_df = sellers_df.join(
    location_lookup_df,
    sellers_df["seller_zip_code_prefix"] == location_lookup_df["location_zip_code_prefix"],
    how="left",
)
dim_seller_df = dim_seller_df.withColumn(
    "location_key", F.coalesce(F.col("location_key"), F.lit(-1).cast("long"))
).drop("location_zip_code_prefix")

# 5. Deterministic hash-based key and location resolution indicator.
dim_seller_df = dim_seller_df.withColumn("seller_key", F.xxhash64(F.col("seller_id")))
dim_seller_df = dim_seller_df.withColumn(
    "has_resolved_location", F.col("location_key") != -1
)

# 6. Preserve the existing Silver quality flag and add Gold validation.
# An unresolved location does not, by itself, invalidate a seller.
dim_seller_df = dim_seller_df.withColumn(
    "dq_seller_gold_valid",
    F.col("seller_id").isNotNull()
    & F.col("seller_key").isNotNull()
    & F.col("seller_zip_code_prefix").isNotNull()
    & F.col("seller_city").isNotNull()
    & F.col("seller_state").isNotNull()
    & F.col("dq_seller_valid"),
)

# 7. Audit and lineage metadata.
dim_seller_df = (
    dim_seller_df.withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Silver"))
    .withColumn("source_dataset", F.lit("sellers"))
)

# 8. Final output schema.
dim_seller_df = dim_seller_df.select(
    "seller_key",
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
    "location_key",
    "has_resolved_location",
    "dq_seller_valid",
    "dq_seller_gold_valid",
    "gold_processed_at",
    "source_layer",
    "source_dataset",
)

# 9. Full overwrite of Gold seller dimension.
(
    dim_seller_df.write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)
job.commit()
