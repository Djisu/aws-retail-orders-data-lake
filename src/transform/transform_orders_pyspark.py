from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lit, when

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = str(PROJECT_ROOT / "data" / "raw")
CURATED_DIR = str(PROJECT_ROOT / "data" / "curated")
REJECTED_DIR = str(PROJECT_ROOT / "data" / "rejected")

def main():
    spark = (
        SparkSession.builder
        .appName("RetailOrdersDataLake")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    print(f"Reading raw CSV files from: {RAW_DIR}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{RAW_DIR}/*.csv")
    )

    print("\n=== Raw Data Preview ===")
    df.show(truncate=False)

    # Standardize / trim string columns
    string_columns = ["customer_name", "region", "product", "payment_method"]
    for c in string_columns:
        df = df.withColumn(c, trim(col(c)))

    # Add rejection reason
    validated_df = (
        df.withColumn(
            "rejection_reason",
            when(col("order_id").isNull(), lit("NULL_ORDER_ID"))
            .when(col("quantity") <= 0, lit("INVALID_QUANTITY"))
            .when(col("unit_price") <= 0, lit("INVALID_UNIT_PRICE"))
            .otherwise(lit(None))
        )
    )

    # Separate valid and rejected rows
    rejected_df = validated_df.filter(col("rejection_reason").isNotNull())
    valid_df = validated_df.filter(col("rejection_reason").isNull()).drop("rejection_reason")

    # Deduplicate by order_id
    deduped_df = valid_df.dropDuplicates(["order_id"])

    # Add derived column
    final_df = deduped_df.withColumn("total_amount", col("quantity") * col("unit_price"))

    print("\n=== Cleaned / Curated Data Preview ===")
    final_df.show(truncate=False)

    print("\n=== Rejected Data Preview ===")
    rejected_df.show(truncate=False)

    # Write curated output
    (
        final_df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(CURATED_DIR)
    )

    # Write rejected rows for audit / troubleshooting
    (
        rejected_df.write
        .mode("overwrite")
        .option("header", True)
        .csv(REJECTED_DIR)
    )

    # Simple metrics
    raw_count = df.count()
    valid_count = final_df.count()
    rejected_count = rejected_df.count()

    print("\n=== Pipeline Metrics ===")
    print(f"Raw row count      : {raw_count}")
    print(f"Curated row count  : {valid_count}")
    print(f"Rejected row count : {rejected_count}")

    print(f"\nCurated Parquet data written to: {CURATED_DIR}")
    print(f"Rejected records written to   : {REJECTED_DIR}")

    spark.stop()

if __name__ == "__main__":
    main()
