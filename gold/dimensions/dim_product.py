"""ShopEasy Gold product dimension (AWS Glue / PySpark).

Replace YOUR_SHOPEASY_BUCKET with your own S3 bucket before running.
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
PRODUCTS_TABLE = "products"
CATEGORY_TABLE = "category_translation"
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/dim_product/"

# 1. Read Silver products
products_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=PRODUCTS_TABLE,
    transformation_ctx="silver_products_source",
)
# Convert Glue DynamicFrame to Spark DataFrame
# and align Silver columns with Gold requirements.
products_df = (
    products_dyf.toDF()
    .withColumnRenamed("product_name_lenght", "product_name_length")
    .withColumnRenamed("product_description_lenght", "product_description_length")
    .withColumn(
        "dq_product_valid",
        F.col("dq_status") == "VALID"
    )
    .withColumn(
        "product_volume_cm3",
        F.col("product_length_cm")
        * F.col("product_height_cm")
        * F.col("product_width_cm")
    )
    .withColumn(
        "has_product_category",
        F.col("product_category_name").isNotNull()
    )
)

# 2. Read Silver category translations
category_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=CATEGORY_TABLE,
    transformation_ctx="silver_category_translation_source",
)
category_df = category_dyf.toDF()

# 3. Select required category lookup columns
category_df = category_df.select(
    "product_category_name", "product_category_name_english"
)

# 4. Join products with category translation
# The category lookup is expected to have one row per category name.
dim_product_df = products_df.join(
    category_df, on="product_category_name", how="left"
)

# 5. Reporting category: English -> Portuguese -> unknown
dim_product_df = dim_product_df.withColumn(
    "reporting_category_name",
    F.coalesce(
        F.col("product_category_name_english"),
        F.col("product_category_name"),
        F.lit("unknown"),
    ),
)

# 6. Translation availability flag
dim_product_df = dim_product_df.withColumn(
    "has_english_translation",
    F.col("product_category_name_english").isNotNull(),
)

# 7. Deterministic hash-based surrogate key
dim_product_df = dim_product_df.withColumn(
    "product_key", F.xxhash64(F.col("product_id"))
)

# 8. Gold-level DQ flag; missing category does not invalidate a product
dim_product_df = dim_product_df.withColumn(
    "dq_product_gold_valid",
    (
        F.col("product_id").isNotNull()
        & F.col("product_key").isNotNull()
        & F.col("dq_product_valid")
    ),
)

# 9. Audit / lineage columns
dim_product_df = (
    dim_product_df
    .withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Silver"))
    .withColumn("source_dataset", F.lit("products,category_translation"))
)

# 10. Final column selection
dim_product_df = dim_product_df.select(
    "product_key",
    "product_id",
    "product_category_name",
    "product_category_name_english",
    "reporting_category_name",
    "product_name_length",
    "product_description_length",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "product_volume_cm3",
    "has_product_category",
    "has_english_translation",
    "dq_product_valid",
    "dq_product_gold_valid",
    "gold_processed_at",
    "source_layer",
    "source_dataset",
)

# 11. Write Gold Parquet
(
    dim_product_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)

job.commit()
