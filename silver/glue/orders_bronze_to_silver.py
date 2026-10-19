import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ============================================================
# 1. Glue job initialization
# ============================================================

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "process_date",
        "load_type"
    ]
)

PROCESS_DATE = args["process_date"]
LOAD_TYPE = args["load_type"].strip().upper()

if LOAD_TYPE not in ("FULL", "INCREMENTAL"):
    raise ValueError(
        f"Unsupported load_type: {LOAD_TYPE}"
    )

print(f"Load type: {LOAD_TYPE}")
print(f"Process date: {PROCESS_DATE}")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


# ============================================================
# 2. Configuration
# ============================================================

BRONZE_PATH = (
    "s3://YOUR_SHOPEASY_BUCKET/Bronze/orders/"
)

SILVER_PATH = (
    "s3://YOUR_SHOPEASY_BUCKET/Silver/orders/"
)

QUARANTINE_PATH = (
    "s3://YOUR_SHOPEASY_BUCKET/Quarantine/orders/"
)


# ============================================================
# 3. Dynamic partition overwrite
# ============================================================
#
# This is CRITICAL for idempotency.
#
# With dynamic mode:
#
# process_date = 2018-01-15
#
# only:
#
# order_purchase_date=2018-01-15
#
# is replaced.
#
# Other Silver partitions remain untouched.
# ============================================================

spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)


print("===================================================")
print(f"SHOPEASY ORDERS {LOAD_TYPE} LOAD")
print("===================================================")
print(f"Process date: {PROCESS_DATE}")
print("===================================================")


# ============================================================
# 4. Read Bronze
# ============================================================
#
# NOTE:
# Current ShopEasy Bronze is still one historical CSV dataset.
#
# Therefore this version is logically incremental but still
# scans the Bronze source.
#
# Later we can partition Bronze by ingestion_date so that
# the source read itself also becomes incremental.
# ============================================================

orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "false")
    .option("mode", "PERMISSIVE")
    .csv(BRONZE_PATH)
)


# ============================================================
# 5. Clean string columns
# ============================================================

string_columns = [
    "order_id",
    "customer_id",
    "order_status"
]

for column_name in string_columns:

    orders_df = orders_df.withColumn(
        column_name,
        F.when(
            F.trim(F.col(column_name)) == "",
            None
        ).otherwise(
            F.trim(F.col(column_name))
        )
    )


# ============================================================
# 6. Convert timestamps
# ============================================================

timestamp_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column_name in timestamp_columns:

    orders_df = orders_df.withColumn(
        column_name,
        F.to_timestamp(
            F.col(column_name),
            "yyyy-MM-dd HH:mm:ss"
        )
    )


# ============================================================
# 7. Create partition column
# ============================================================

orders_df = orders_df.withColumn(
    "order_purchase_date",
    F.to_date(
        F.col("order_purchase_timestamp")
    )
)


# ============================================================
# 8. Select records based on load type
# ============================================================

if LOAD_TYPE == "FULL":

    print("FULL LOAD: Processing all historical orders")

    incremental_df = orders_df

else:

    print(f"INCREMENTAL LOAD: Processing {PROCESS_DATE}")

    incremental_df = orders_df.filter(
        F.col("order_purchase_date")
        == F.to_date(F.lit(PROCESS_DATE))
    )


source_count = incremental_df.count()


print("===================================================")
print(f"{LOAD_TYPE} SOURCE")
print("===================================================")
print(f"process_date : {PROCESS_DATE}")
print(f"source_count : {source_count}")
print("===================================================")


# ============================================================
# 9. Safety check
# ============================================================

if source_count == 0:

    if LOAD_TYPE == "FULL":
        raise RuntimeError(
            "FULL load aborted: Bronze orders dataset is empty."
        )

    else:
        print(
            f"No orders found for process_date={PROCESS_DATE}. "
            "No Silver partition will be written."
        )

        job.commit()
        sys.exit(0)


# ============================================================
# 10. Duplicate protection
# ============================================================
#
# Order grain = one row per order_id.
#
# This protects Silver against accidental duplicate records
# within the incoming processing batch.
# ============================================================

incremental_df = incremental_df.dropDuplicates(
    ["order_id"]
)


deduped_count = incremental_df.count()


print(
    f"Count after order_id deduplication: "
    f"{deduped_count}"
)


# ============================================================
# 11. DQ Rule 1
# approval before purchase
# ============================================================

incremental_df = incremental_df.withColumn(
    "dq_approved_before_purchase",
    F.when(
        F.col("order_approved_at").isNotNull()
        & F.col("order_purchase_timestamp").isNotNull()
        & (
            F.col("order_approved_at")
            < F.col("order_purchase_timestamp")
        ),
        F.lit(1)
    ).otherwise(F.lit(0))
)


# ============================================================
# 12. DQ Rule 2
# carrier before approval
# ============================================================

incremental_df = incremental_df.withColumn(
    "dq_carrier_before_approval",
    F.when(
        F.col("order_delivered_carrier_date").isNotNull()
        & F.col("order_approved_at").isNotNull()
        & (
            F.col("order_delivered_carrier_date")
            < F.col("order_approved_at")
        ),
        F.lit(1)
    ).otherwise(F.lit(0))
)


# ============================================================
# 13. DQ Rule 3
# delivery before carrier
# ============================================================

incremental_df = incremental_df.withColumn(
    "dq_delivery_before_carrier",
    F.when(
        F.col("order_delivered_customer_date").isNotNull()
        & F.col("order_delivered_carrier_date").isNotNull()
        & (
            F.col("order_delivered_customer_date")
            < F.col("order_delivered_carrier_date")
        ),
        F.lit(1)
    ).otherwise(F.lit(0))
)


# ============================================================
# 14. Overall DQ status
# ============================================================

incremental_df = incremental_df.withColumn(
    "dq_status",
    F.when(
        (F.col("dq_approved_before_purchase") == 1)
        | (F.col("dq_carrier_before_approval") == 1)
        | (F.col("dq_delivery_before_carrier") == 1),
        F.lit("INVALID")
    ).otherwise(
        F.lit("VALID")
    )
)


# ============================================================
# 15. DQ error code
# ============================================================

incremental_df = incremental_df.withColumn(
    "dq_error_code",
    F.when(
        F.col("dq_approved_before_purchase") == 1,
        F.lit("APPROVED_BEFORE_PURCHASE")
    )
    .when(
        F.col("dq_carrier_before_approval") == 1,
        F.lit("CARRIER_BEFORE_APPROVAL")
    )
    .when(
        F.col("dq_delivery_before_carrier") == 1,
        F.lit("DELIVERY_BEFORE_CARRIER")
    )
    .otherwise(
        F.lit(None).cast(StringType())
    )
)


# ============================================================
# 16. DQ error message
# ============================================================

incremental_df = incremental_df.withColumn(
    "dq_error_message",
    F.when(
        F.col("dq_approved_before_purchase") == 1,
        F.lit(
            "Order approval timestamp is earlier than "
            "order purchase timestamp"
        )
    )
    .when(
        F.col("dq_carrier_before_approval") == 1,
        F.lit(
            "Carrier delivery timestamp is earlier than "
            "order approval timestamp"
        )
    )
    .when(
        F.col("dq_delivery_before_carrier") == 1,
        F.lit(
            "Customer delivery timestamp is earlier than "
            "carrier delivery timestamp"
        )
    )
    .otherwise(
        F.lit(None).cast(StringType())
    )
)


# ============================================================
# 17. Audit columns
# ============================================================

incremental_df = (
    incremental_df
    .withColumn(
        "source_dataset",
        F.lit("orders")
    )
    .withColumn(
    "process_date",
    F.when(
        F.lit(LOAD_TYPE) == "FULL",
        F.current_date()
    ).otherwise(
        F.to_date(F.lit(PROCESS_DATE))
        )
    )
    .withColumn(
        "silver_processed_timestamp",
        F.current_timestamp()
    )
)


# ============================================================
# 18. Split valid / invalid records
# ============================================================

valid_df = incremental_df.filter(
    F.col("dq_status") == "VALID"
)

invalid_df = incremental_df.filter(
    F.col("dq_status") == "INVALID"
)


total_count = incremental_df.count()
valid_count = valid_df.count()
invalid_count = invalid_df.count()


print("===================================================")
print(f"{LOAD_TYPE} DATA QUALITY SUMMARY")
print("===================================================")
print(f"Process Date     : {PROCESS_DATE}")
print(f"Total            : {total_count}")
print(f"Valid            : {valid_count}")
print(f"Invalid          : {invalid_count}")
print("===================================================")


# ============================================================
# 19. DQ reconciliation
# ============================================================

if total_count != valid_count + invalid_count:

    raise Exception(
        "DQ reconciliation failed: "
        f"Total={total_count}, "
        f"Valid={valid_count}, "
        f"Invalid={invalid_count}"
    )


# ============================================================
# 20. Write Silver orders
# ============================================================

if LOAD_TYPE == "FULL":

    print("FULL LOAD: Rebuilding all Silver orders partitions")

    # Static overwrite replaces the complete Silver dataset.
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "static"
    )

else:

    print("INCREMENTAL LOAD: Replacing selected date partition")

    # Dynamic overwrite preserves other date partitions.
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )

# Execute the write for BOTH FULL and INCREMENTAL loads.
(
    incremental_df.write
    .mode("overwrite")
    .partitionBy("order_purchase_date")
    .option("compression", "snappy")
    .parquet(SILVER_PATH)
)

print(f"Silver write completed: {SILVER_PATH} | rows={total_count} | mode={LOAD_TYPE}")


# ============================================================
# 21. Write quarantine records
# ============================================================

if LOAD_TYPE == "FULL":
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "static"
    )
else:
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )

if invalid_count > 0:

    quarantine_df = (
        invalid_df
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
    )

    (
        quarantine_df.write
        .mode("overwrite")
        .partitionBy("order_purchase_date")
        .option("compression", "snappy")
        .parquet(QUARANTINE_PATH)
    )

    print(
        f"{invalid_count} invalid records written "
        f"to quarantine"
    )

else:
    print("No invalid records found")

    if LOAD_TYPE == "FULL":
        print(
            "FULL load has no invalid records. "
            "Quarantine cleanup will be handled "
            "before the job is executed."
        )


# ============================================================
# 22. Final logging
# ============================================================

print("===================================================")
print(f"{LOAD_TYPE} ORDERS LOAD COMPLETED")
print("===================================================")
print(f"Process Date : {PROCESS_DATE}")
print(f"Rows Written : {total_count}")
print(f"Valid        : {valid_count}")
print(f"Invalid      : {invalid_count}")
print("===================================================")


job.commit()
