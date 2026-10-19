import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# AWS Glue job initialization
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Replace the placeholder with your own S3 bucket before running.
BRONZE_DATABASE = "shopeasy_bronze_dev"
BRONZE_TABLE = "geolocation"
SILVER_PATH = "s3://YOUR_SHOPEASY_BUCKET/Silver/geolocation/"

# Read raw geolocation data registered in the Bronze Glue Catalog.
bronze_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_geolocation_source",
)
df = bronze_dyf.toDF()

# Normalize city/state strings and treat empty values as null.
for column_name in ["geolocation_city", "geolocation_state"]:
    df = df.withColumn(
        column_name,
        F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(
            F.trim(F.col(column_name))
        ),
    )

# ZIP prefix is an identifier, not a numeric measure.
df = df.withColumn(
    "geolocation_zip_code_prefix", F.col("geolocation_zip_code_prefix").cast("string")
)
df = df.withColumn("geolocation_lat", F.col("geolocation_lat").cast("double"))
df = df.withColumn("geolocation_lng", F.col("geolocation_lng").cast("double"))

df = df.withColumn("geolocation_city", F.lower(F.trim(F.col("geolocation_city"))))
df = df.withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))

# Data-quality checks. Invalid rows remain in the Silver output with DQ flags.
missing_zip = F.col("geolocation_zip_code_prefix").isNull()
missing_city = F.col("geolocation_city").isNull()
missing_state = F.col("geolocation_state").isNull()
invalid_latitude = (
    F.col("geolocation_lat").isNull()
    | (F.col("geolocation_lat") < -90)
    | (F.col("geolocation_lat") > 90)
)
invalid_longitude = (
    F.col("geolocation_lng").isNull()
    | (F.col("geolocation_lng") < -180)
    | (F.col("geolocation_lng") > 180)
)

df = df.withColumn(
    "dq_geolocation_valid",
    ~(missing_zip | missing_city | missing_state | invalid_latitude | invalid_longitude),
)
df = df.withColumn(
    "dq_issue_code",
    F.when(missing_zip, F.lit("MISSING_ZIP_CODE"))
    .when(missing_city, F.lit("MISSING_CITY"))
    .when(missing_state, F.lit("MISSING_STATE"))
    .when(invalid_latitude, F.lit("INVALID_LATITUDE"))
    .when(invalid_longitude, F.lit("INVALID_LONGITUDE"))
    .otherwise(F.lit(None)),
)

# Lineage metadata.
df = df.withColumn("silver_processed_at", F.current_timestamp())
df = df.withColumn("source_layer", F.lit("Bronze"))
df = df.withColumn("source_dataset", F.lit("geolocation"))

# Full refresh of Snappy-compressed Silver Parquet data.
(
    df.write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(SILVER_PATH)
)

job.commit()
