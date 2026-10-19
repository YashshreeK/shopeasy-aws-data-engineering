"""ShopEasy reviews Bronze-to-Silver AWS Glue ETL.

Public portfolio version. Configure the bucket name for your own deployment.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# Replace with the S3 bucket used in your own AWS deployment.
S3_BUCKET = "YOUR_SHOPEASY_BUCKET"
BRONZE_PATH = f"s3://{S3_BUCKET}/Bronze/reviews/olist_order_reviews_dataset.csv"
SILVER_PATH = f"s3://{S3_BUCKET}/Silver/reviews/"

# Read multiline, quoted CSV reviews (comments may include newlines).
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", '"')
    .csv(BRONZE_PATH)
)
print("BRONZE_SPARK_ROW_COUNT:", df.count())

string_columns = [
    "review_id", "order_id", "review_comment_title", "review_comment_message",
    "review_creation_date", "review_answer_timestamp"
]
for column_name in string_columns:
    df = df.withColumn(
        column_name,
        F.when(F.trim(F.col(column_name)) == "", F.lit(None)).otherwise(
            F.trim(F.col(column_name))
        ),
    )

df = df.withColumn("review_score", F.col("review_score").cast("int"))
for column_name in ["review_creation_date", "review_answer_timestamp"]:
    df = df.withColumn(column_name, F.col(column_name).cast(TimestampType()))

# Deduplication intentionally disabled in the original implementation.
# df = df.dropDuplicates(["review_id", "order_id"])

missing_review_id = F.col("review_id").isNull()
missing_order_id = F.col("order_id").isNull()
invalid_review_score = (
    F.col("review_score").isNull()
    | (F.col("review_score") < 1)
    | (F.col("review_score") > 5)
)
missing_creation_date = F.col("review_creation_date").isNull()
answer_before_creation = (
    F.col("review_answer_timestamp").isNotNull()
    & F.col("review_creation_date").isNotNull()
    & (F.col("review_answer_timestamp") < F.col("review_creation_date"))
)

df = df.withColumn(
    "dq_review_valid",
    ~(
        missing_review_id | missing_order_id | invalid_review_score
        | missing_creation_date | answer_before_creation
    ),
)
df = df.withColumn(
    "dq_issue_code",
    F.when(missing_review_id, F.lit("MISSING_REVIEW_ID"))
    .when(missing_order_id, F.lit("MISSING_ORDER_ID"))
    .when(invalid_review_score, F.lit("INVALID_REVIEW_SCORE"))
    .when(missing_creation_date, F.lit("MISSING_REVIEW_CREATION_DATE"))
    .when(answer_before_creation, F.lit("ANSWER_BEFORE_REVIEW_CREATION"))
    .otherwise(F.lit(None)),
)

df = df.withColumn("has_review_title", F.col("review_comment_title").isNotNull())
df = df.withColumn("has_review_comment", F.col("review_comment_message").isNotNull())
df = df.withColumn("silver_processed_at", F.current_timestamp())
df = df.withColumn("source_layer", F.lit("Bronze"))
df = df.withColumn("source_dataset", F.lit("reviews"))

(
    df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(SILVER_PATH)
)
job.commit()
