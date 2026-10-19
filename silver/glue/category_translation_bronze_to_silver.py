import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F


# Glue job initialization
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Configuration: replace the placeholder with your own S3 bucket at deployment.
BRONZE_DATABASE = "shopeasy_bronze_dev"
BRONZE_TABLE = "category_translation"
SILVER_PATH = "s3://YOUR_SHOPEASY_BUCKET/Silver/category_translation/"

# 1. Read Bronze reference data via the Glue Data Catalog.
bronze_dyf = glue_context.create_dynamic_frame.from_catalog(
    database=BRONZE_DATABASE,
    table_name=BRONZE_TABLE,
    transformation_ctx="bronze_category_translation_source",
)
df = bronze_dyf.toDF()

# 2. Normalize both category labels.
string_columns = [
    "product_category_name",
    "product_category_name_english",
]
for column_name in string_columns:
    df = df.withColumn(
        column_name,
        F.when(
            F.trim(F.col(column_name)) == "",
            F.lit(None),
        ).otherwise(F.lower(F.trim(F.col(column_name)))),
    )

# 3. Deduplicate on the category reference key.
df = df.dropDuplicates(["product_category_name"])

# 4. Define data-quality rules.
missing_category = F.col("product_category_name").isNull()
missing_translation = F.col("product_category_name_english").isNull()

# 5. Attach DQ status and a single prioritized issue code.
df = df.withColumn(
    "dq_category_valid",
    ~(missing_category | missing_translation),
)
df = df.withColumn(
    "dq_issue_code",
    F.when(missing_category, F.lit("MISSING_CATEGORY_NAME"))
    .when(missing_translation, F.lit("MISSING_ENGLISH_TRANSLATION"))
    .otherwise(F.lit(None)),
)

# 6. Add audit and lineage fields.
df = df.withColumn("silver_processed_at", F.current_timestamp())
df = df.withColumn("source_layer", F.lit("Bronze"))
df = df.withColumn("source_dataset", F.lit("category_translation"))

# 7. Write the complete Silver dataset as Snappy-compressed Parquet.
(
    df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(SILVER_PATH)
)

job.commit()
