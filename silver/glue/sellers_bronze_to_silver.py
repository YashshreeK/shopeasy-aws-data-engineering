"""ShopEasy sellers Bronze-to-Silver AWS Glue ETL job.

Configure YOUR_SHOPEASY_BUCKET before running this script.
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

BRONZE_DATABASE = "shopeasy_bronze_dev"
BRONZE_TABLE = "sellers"
SILVER_PATH = "s3://YOUR_SHOPEASY_BUCKET/Silver/sellers/"

# 1. Read Bronze sellers from the Glue Data Catalog.
bronze_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_sellers_source",
)
df = bronze_dyf.toDF()

# 2. Clean string columns.
string_columns = ["seller_id", "seller_city", "seller_state"]
for column_name in string_columns:
    df = df.withColumn(
        column_name,
        F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(
            F.trim(F.col(column_name))
        ),
    )

# 3. Keep ZIP prefix as string.
df = df.withColumn(
    "seller_zip_code_prefix", F.col("seller_zip_code_prefix").cast("string")
)

# 4. Standardize city and state.
df = df.withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
df = df.withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))

# 5. Deduplicate on seller_id.
df = df.dropDuplicates(["seller_id"])

# 6. Define data-quality rules.
missing_seller_id = F.col("seller_id").isNull()
missing_location = (
    F.col("seller_city").isNull()
    | F.col("seller_state").isNull()
    | F.col("seller_zip_code_prefix").isNull()
)

# 7. Add data-quality fields.
df = df.withColumn("dq_seller_valid", ~(missing_seller_id | missing_location))
df = df.withColumn(
    "dq_issue_code",
    F.when(missing_seller_id, F.lit("MISSING_SELLER_ID"))
    .when(missing_location, F.lit("MISSING_SELLER_LOCATION"))
    .otherwise(F.lit(None)),
)

# 8. Add audit and lineage fields.
df = df.withColumn("silver_processed_at", F.current_timestamp())
df = df.withColumn("source_layer", F.lit("Bronze"))
df = df.withColumn("source_dataset", F.lit("sellers"))

# 9. Write Silver Parquet.
(
    df.write.mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(SILVER_PATH)
)

job.commit()
