"""ShopEasy Bronze-to-Silver products transformation (AWS Glue / PySpark).

Configure YOUR_SHOPEASY_BUCKET before running in your AWS environment.
"""
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BRONZE_PATH = "s3://YOUR_SHOPEASY_BUCKET/Bronze/products/"
SILVER_PATH = "s3://YOUR_SHOPEASY_BUCKET/Silver/products/"
QUARANTINE_PATH = "s3://YOUR_SHOPEASY_BUCKET/Quarantine/products/"

products_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .csv(BRONZE_PATH)
)
bronze_count = products_df.count()
print(f"Bronze products row count: {bronze_count}")
products_df.printSchema()

for column_name in ["product_id", "product_category_name"]:
    products_df = products_df.withColumn(
        column_name,
        F.when(F.trim(F.col(column_name)) == "", None).otherwise(
            F.trim(F.col(column_name))
        ),
    )

for column_name in [
    "product_name_lenght", "product_description_lenght", "product_photos_qty"
]:
    products_df = products_df.withColumn(column_name, F.col(column_name).cast("int"))

for column_name in [
    "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
]:
    products_df = products_df.withColumn(column_name, F.col(column_name).cast("double"))

products_df = products_df.withColumn(
    "dq_invalid_product_weight",
    F.when(
        F.col("product_weight_g").isNull() | (F.col("product_weight_g") <= 0),
        F.lit(1),
    ).otherwise(F.lit(0)),
)
products_df = products_df.withColumn(
    "dq_status",
    F.when(F.col("dq_invalid_product_weight") == 1, F.lit("INVALID")).otherwise(
        F.lit("VALID")
    ),
)
products_df = products_df.withColumn(
    "dq_error_code",
    F.when(
        F.col("dq_invalid_product_weight") == 1, F.lit("INVALID_PRODUCT_WEIGHT")
    ).otherwise(F.lit(None).cast(StringType())),
)
products_df = products_df.withColumn(
    "dq_error_message",
    F.when(F.col("product_weight_g").isNull(), F.lit("Product weight is null"))
    .when(
        F.col("product_weight_g") <= 0,
        F.lit("Product weight must be greater than zero"),
    )
    .otherwise(F.lit(None).cast(StringType())),
)
products_df = (
    products_df.withColumn("source_dataset", F.lit("products"))
    .withColumn("silver_processed_timestamp", F.current_timestamp())
)
valid_products_df = products_df.filter(F.col("dq_status") == "VALID")
invalid_products_df = products_df.filter(F.col("dq_status") == "INVALID")

silver_count = products_df.count()
valid_count = valid_products_df.count()
invalid_count = invalid_products_df.count()
print(
    f"Bronze={bronze_count}, Silver={silver_count}, "
    f"Valid={valid_count}, Invalid={invalid_count}"
)

(
    products_df.write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(SILVER_PATH)
)
quarantine_df = invalid_products_df.withColumn(
    "quarantine_timestamp", F.current_timestamp()
)
(
    quarantine_df.write.mode("overwrite")
    .option("compression", "snappy")
    .parquet(QUARANTINE_PATH)
)

if bronze_count != silver_count:
    raise Exception(
        f"Source-to-Silver reconciliation failed. Bronze={bronze_count}, Silver={silver_count}"
    )
if silver_count != valid_count + invalid_count:
    raise Exception(
        f"DQ reconciliation failed. Silver={silver_count}, "
        f"Valid={valid_count}, Invalid={invalid_count}"
    )
print("PRODUCT SILVER + QUARANTINE PROCESSING SUCCESSFUL")
job.commit()
