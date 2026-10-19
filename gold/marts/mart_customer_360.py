"""ShopEasy Gold Customer 360 mart.

Requires Gold dim_customer and fact_order_items in the Glue Data Catalog.
Replace YOUR_SHOPEASY_BUCKET with your own S3 bucket before deployment.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================================================
# 1. Initialize Glue
# =========================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)


GOLD_DATABASE = "shopeasy_gold_dev"

GOLD_PATH = (
    "s3://YOUR_SHOPEASY_BUCKET/"
    "Gold/mart_customer_360/"
)


# =========================================================
# 2. Read Gold dim_customer
# =========================================================

customer_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="dim_customer",
        transformation_ctx="dim_customer_source"
    )
    .toDF()
)


# =========================================================
# 3. Read Gold fact_order_items
# =========================================================

fact_df = (
    glue_context.create_dynamic_frame.from_catalog(
        database=GOLD_DATABASE,
        table_name="fact_order_items",
        transformation_ctx="fact_order_items_source"
    )
    .toDF()
)


# =========================================================
# 4. Select required customer fields
# =========================================================

customer_df = customer_df.select(
    "customer_key",
    "customer_unique_id",

    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",

    "first_order_date",
    "last_order_date",
    "total_orders",

    "dq_customer_gold_valid"
)


# =========================================================
# 5. Select required fact fields
# =========================================================

fact_df = fact_df.select(
    "order_id",
    "customer_key",

    "order_purchase_timestamp",

    "price",
    "freight_value",
    "item_total_value",

    "is_late_delivery"
)


# =========================================================
# 6. Determine dataset's latest transaction date
#
# Important:
# Recency should be relative to the dataset snapshot,
# not today's date.
# =========================================================

max_date_df = fact_df.agg(
    F.max(
        F.to_date("order_purchase_timestamp")
    ).alias("dataset_max_order_date")
)


# =========================================================
# 7. Aggregate customer transaction metrics
# =========================================================

customer_metrics_df = (
    fact_df
    .groupBy("customer_key")
    .agg(

        # Number of actual orders
        F.countDistinct("order_id")
        .alias("orders_from_fact"),

        # Item volume
        F.count("*")
        .alias("total_items"),

        # Monetary metrics
        F.sum("price")
        .alias("lifetime_item_revenue"),

        F.sum("freight_value")
        .alias("lifetime_freight_value"),

        F.sum("item_total_value")
        .alias("lifetime_order_value"),

        # First / last transaction observed in fact
        F.min(
            F.to_date("order_purchase_timestamp")
        ).alias("fact_first_order_date"),

        F.max(
            F.to_date("order_purchase_timestamp")
        ).alias("fact_last_order_date"),

        # Count late orders correctly at order grain
        F.countDistinct(
            F.when(
                F.col("is_late_delivery") == True,
                F.col("order_id")
            )
        ).alias("late_orders")
    )
)


# =========================================================
# 8. Join dimension + customer metrics
# =========================================================

mart_df = customer_df.join(
    customer_metrics_df,
    on="customer_key",
    how="left"
)


# =========================================================
# 9. Add dataset maximum date
# =========================================================

mart_df = mart_df.crossJoin(max_date_df)


# =========================================================
# 10. Handle customers with no fact rows safely
#
# We don't expect this here, but defensive handling makes
# the mart more robust.
# =========================================================

mart_df = (
    mart_df

    .withColumn(
        "orders_from_fact",
        F.coalesce(
            F.col("orders_from_fact"),
            F.lit(0)
        )
    )

    .withColumn(
        "total_items",
        F.coalesce(
            F.col("total_items"),
            F.lit(0)
        )
    )

    .withColumn(
        "lifetime_item_revenue",
        F.coalesce(
            F.col("lifetime_item_revenue"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "lifetime_freight_value",
        F.coalesce(
            F.col("lifetime_freight_value"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "lifetime_order_value",
        F.coalesce(
            F.col("lifetime_order_value"),
            F.lit(0.0)
        )
    )

    .withColumn(
        "late_orders",
        F.coalesce(
            F.col("late_orders"),
            F.lit(0)
        )
    )
)


# =========================================================
# 11. Derived customer KPIs
# =========================================================

mart_df = (

    mart_df

    # ---------------------------------------------
    # Average order value
    # ---------------------------------------------

    .withColumn(
        "avg_order_value",
        F.when(
            F.col("orders_from_fact") > 0,
            F.round(
                F.col("lifetime_order_value")
                / F.col("orders_from_fact"),
                2
            )
        ).otherwise(F.lit(0.0))
    )

    # ---------------------------------------------
    # Average items per order
    # ---------------------------------------------

    .withColumn(
        "avg_items_per_order",
        F.when(
            F.col("orders_from_fact") > 0,
            F.round(
                F.col("total_items")
                / F.col("orders_from_fact"),
                2
            )
        ).otherwise(F.lit(0.0))
    )

    # ---------------------------------------------
    # Freight percentage of spend
    # ---------------------------------------------

    .withColumn(
        "freight_pct_of_value",
        F.when(
            F.col("lifetime_order_value") > 0,
            F.round(
                (
                    F.col("lifetime_freight_value")
                    / F.col("lifetime_order_value")
                ) * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )

    # ---------------------------------------------
    # Late-order percentage
    # ---------------------------------------------

    .withColumn(
        "late_order_pct",
        F.when(
            F.col("orders_from_fact") > 0,
            F.round(
                (
                    F.col("late_orders")
                    / F.col("orders_from_fact")
                ) * 100,
                2
            )
        ).otherwise(F.lit(0.0))
    )

    # ---------------------------------------------
    # Customer recency
    #
    # Dataset max date - customer's last order
    # ---------------------------------------------

    .withColumn(
        "recency_days",
        F.datediff(
            F.col("dataset_max_order_date"),
            F.col("fact_last_order_date")
        )
    )

    # ---------------------------------------------
    # Customer tenure
    # ---------------------------------------------

    .withColumn(
        "customer_tenure_days",
        F.datediff(
            F.col("fact_last_order_date"),
            F.col("fact_first_order_date")
        )
    )

    # ---------------------------------------------
    # Repeat customer
    # ---------------------------------------------

    .withColumn(
        "is_repeat_customer",
        F.col("orders_from_fact") > 1
    )
)


# =========================================================
# 12. Frequency segment
# =========================================================

mart_df = mart_df.withColumn(
    "frequency_segment",

    F.when(
        F.col("orders_from_fact") == 1,
        F.lit("One-Time")
    )

    .when(
        F.col("orders_from_fact").between(2, 4),
        F.lit("Repeat")
    )

    .when(
        F.col("orders_from_fact") >= 5,
        F.lit("Loyal")
    )

    .otherwise(
        F.lit("No Orders")
    )
)


# =========================================================
# 13. Value segment
#
# Use quartiles so segmentation adapts to the dataset.
#
# Q4 = highest spending customers.
# =========================================================

value_window = Window.orderBy(
    F.col("lifetime_order_value").asc(),
    F.col("customer_key").asc()
)


mart_df = mart_df.withColumn(
    "value_quartile",
    F.ntile(4).over(value_window)
)


mart_df = mart_df.withColumn(
    "value_segment",

    F.when(
        F.col("value_quartile") == 4,
        F.lit("High Value")
    )

    .when(
        F.col("value_quartile") == 3,
        F.lit("Upper Mid Value")
    )

    .when(
        F.col("value_quartile") == 2,
        F.lit("Lower Mid Value")
    )

    .otherwise(
        F.lit("Low Value")
    )
)


# =========================================================
# 14. Customer engagement segment
# =========================================================

mart_df = mart_df.withColumn(
    "customer_segment",

    F.when(
        F.col("orders_from_fact") == 0,
        F.lit("No Purchases")
    )

    .when(
        (F.col("orders_from_fact") >= 5)
        & (F.col("value_quartile") == 4),
        F.lit("VIP")
    )

    .when(
        F.col("orders_from_fact") >= 5,
        F.lit("Loyal")
    )

    .when(
        (F.col("orders_from_fact") > 1)
        & (F.col("value_quartile") >= 3),
        F.lit("High-Value Repeat")
    )

    .when(
        F.col("orders_from_fact") > 1,
        F.lit("Repeat")
    )

    .otherwise(
        F.lit("One-Time")
    )
)


# =========================================================
# 15. Round monetary values
# =========================================================

mart_df = (
    mart_df

    .withColumn(
        "lifetime_item_revenue",
        F.round("lifetime_item_revenue", 2)
    )

    .withColumn(
        "lifetime_freight_value",
        F.round("lifetime_freight_value", 2)
    )

    .withColumn(
        "lifetime_order_value",
        F.round("lifetime_order_value", 2)
    )
)

# =========================================================
# 16. Gold DQ
# =========================================================

mart_df = mart_df.withColumn(
    "dq_customer_360_valid",
    (
        F.col("customer_key").isNotNull()
        & F.col("customer_unique_id").isNotNull()
        & (F.col("orders_from_fact") >= 0)
        & (F.col("lifetime_order_value") >= 0)
        & F.col("dq_customer_gold_valid")
    )
)

# =========================================================
# 17. Audit columns
# =========================================================

mart_df = (
    mart_df

    .withColumn(
        "gold_processed_at",
        F.current_timestamp()
    )

    .withColumn(
        "source_layer",
        F.lit("Gold")
    )

    .withColumn(
        "source_dataset",
        F.lit("dim_customer,fact_order_items")
    )
)


# =========================================================
# 18. Final mart schema
# =========================================================

mart_customer_360_df = mart_df.select(

    # Customer identity
    "customer_key",
    "customer_unique_id",

    # Current Type-1 location
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",

    # Lifecycle
    "fact_first_order_date",
    "fact_last_order_date",
    "dataset_max_order_date",

    "customer_tenure_days",
    "recency_days",

    # Frequency
    "orders_from_fact",
    "total_items",
    "is_repeat_customer",

    # Monetary
    "lifetime_item_revenue",
    "lifetime_freight_value",
    "lifetime_order_value",

    "avg_order_value",
    "avg_items_per_order",
    "freight_pct_of_value",

    # Delivery experience
    "late_orders",
    "late_order_pct",

    # Segmentation
    "frequency_segment",
    "value_quartile",
    "value_segment",
    "customer_segment",

    # DQ
    "dq_customer_360_valid",

    # Audit
    "gold_processed_at",
    "source_layer",
    "source_dataset"
)


# =========================================================
# 19. Write Gold mart
# =========================================================

(
    mart_customer_360_df.write
    .mode("overwrite")
    .format("parquet")
    .option("compression", "snappy")
    .save(GOLD_PATH)
)


job.commit()
