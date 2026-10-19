"""Build the ShopEasy Gold location dimension from Silver geolocation data.

Requires the shopeasy_silver_dev.geolocation Glue Catalog table.
Replace YOUR_SHOPEASY_BUCKET before running in your AWS environment.
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

SILVER_DATABASE = "shopeasy_silver_dev"
GEOLOCATION_TABLE = "geolocation"
GOLD_PATH = "s3://YOUR_SHOPEASY_BUCKET/Gold/dim_location/"

# 1. Read Silver geolocation.
geolocation_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=SILVER_DATABASE,
    table_name=GEOLOCATION_TABLE,
    transformation_ctx="silver_geolocation_source",
)
geo_df = geolocation_dyf.toDF().select(
    "geolocation_zip_code_prefix",
    "geolocation_city",
    "geolocation_state",
    "geolocation_lat",
    "geolocation_lng",
    "dq_geolocation_valid",
)

# 2. Aggregate coordinates, source counts, and quality at ZIP-prefix grain.
zip_metrics_df = geo_df.groupBy("geolocation_zip_code_prefix").agg(
    F.avg("geolocation_lat").alias("latitude"),
    F.avg("geolocation_lng").alias("longitude"),
    F.count("*").alias("source_location_records"),
    F.countDistinct("geolocation_city").alias("distinct_city_count"),
    F.countDistinct("geolocation_state").alias("distinct_state_count"),
    F.min(F.col("dq_geolocation_valid").cast("int")).alias(
        "all_source_records_valid"
    ),
)

# 3. Choose the most frequent city/state combination for each ZIP prefix.
city_state_counts_df = geo_df.groupBy(
    "geolocation_zip_code_prefix", "geolocation_city", "geolocation_state"
).agg(F.count("*").alias("city_state_frequency"))

location_window = Window.partitionBy("geolocation_zip_code_prefix").orderBy(
    F.col("city_state_frequency").desc(),
    F.col("geolocation_state").asc(),
    F.col("geolocation_city").asc(),
)
canonical_location_df = (
    city_state_counts_df
    .withColumn("location_rank", F.row_number().over(location_window))
    .filter(F.col("location_rank") == 1)
    .select(
        "geolocation_zip_code_prefix",
        F.col("geolocation_city").alias("city"),
        F.col("geolocation_state").alias("state"),
    )
)

# 4. Build Gold location dimension.
dim_location_df = (
    zip_metrics_df
    .join(canonical_location_df, on="geolocation_zip_code_prefix", how="left")
    .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
    .withColumn("location_key", F.xxhash64(F.col("zip_code_prefix")))
    .withColumn("has_city_ambiguity", F.col("distinct_city_count") > 1)
    .withColumn("has_state_ambiguity", F.col("distinct_state_count") > 1)
    .withColumn(
        "dq_location_valid",
        F.col("zip_code_prefix").isNotNull()
        & F.col("city").isNotNull()
        & F.col("state").isNotNull()
        & F.col("latitude").between(-90, 90)
        & F.col("longitude").between(-180, 180)
        & (F.col("all_source_records_valid") == 1),
    )
    .withColumn("gold_processed_at", F.current_timestamp())
    .withColumn("source_layer", F.lit("Silver"))
    .withColumn("source_dataset", F.lit("geolocation"))
)

# 5. Add a sentinel dimension member for unresolved ZIP-prefix lookups.
unknown_location_df = spark.createDataFrame(
    [(-1, "unknown", "unknown", "unknown", None, None, 0, 0, 0,
      False, False, True, "Silver", "geolocation")],
    """
    location_key long,
    zip_code_prefix string,
    city string,
    state string,
    latitude double,
    longitude double,
    source_location_records long,
    distinct_city_count long,
    distinct_state_count long,
    has_city_ambiguity boolean,
    has_state_ambiguity boolean,
    dq_location_valid boolean,
    source_layer string,
    source_dataset string
    """,
).withColumn("gold_processed_at", F.current_timestamp())

final_columns = [
    "location_key", "zip_code_prefix", "city", "state", "latitude", "longitude",
    "source_location_records", "distinct_city_count", "distinct_state_count",
    "has_city_ambiguity", "has_state_ambiguity", "dq_location_valid",
    "gold_processed_at", "source_layer", "source_dataset",
]

dim_location_df = dim_location_df.select(*final_columns).unionByName(
    unknown_location_df
)

# 6. Overwrite Gold Parquet output.
(
    dim_location_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)

job.commit()
