"""
Silver Layer — Cleanse, Conform, Validate
Reads from bronze Delta tables, applies data quality checks,
deduplicates, casts types, and writes to silver Delta tables.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, to_timestamp, upper, trim, when, lit,
    current_timestamp, regexp_replace, round as spark_round,
)
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("MedallionSilverTransform")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def cleanse_transactions(df: DataFrame) -> DataFrame:
    """
    Apply cleansing rules to raw transaction data:
    - Cast types correctly
    - Standardize strings
    - Remove duplicates on transaction_id
    - Flag nulls and out-of-range amounts
    """
    logger.info("Applying cleansing rules to transactions...")

    cleansed = (
        df
        .withColumn("transaction_id", trim(col("transaction_id")))
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("amount", spark_round(col("amount"), 2))
        .withColumn("transaction_ts", to_timestamp(col("transaction_date"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("customer_id", regexp_replace(col("customer_id"), r"\s+", ""))
        .withColumn(
            "dq_amount_valid",
            when(col("amount") > 0, lit(True)).otherwise(lit(False))
        )
        .withColumn(
            "dq_no_nulls",
            when(
                col("transaction_id").isNull() | col("customer_id").isNull(),
                lit(False)
            ).otherwise(lit(True))
        )
        .withColumn("_silver_timestamp", current_timestamp())
        .withColumn("_layer", lit("silver"))
    )

    deduped = cleansed.dropDuplicates(["transaction_id"])

    before = df.count()
    after = deduped.count()
    logger.info(f"Deduplication: {before:,} → {after:,} records ({before - after:,} removed)")

    return deduped


def validate_and_quarantine(df: DataFrame, silver_path: str, quarantine_path: str) -> None:
    """
    Split records into valid (silver) and invalid (quarantine) sets.
    Only clean records proceed to gold.
    """
    valid_df = df.filter(col("dq_amount_valid") & col("dq_no_nulls"))
    invalid_df = df.filter(~col("dq_amount_valid") | ~col("dq_no_nulls"))

    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    logger.info(f"Valid records: {valid_count:,} | Quarantined: {invalid_count:,}")

    (
        valid_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("currency")
        .save(silver_path)
    )

    if invalid_count > 0:
        (
            invalid_df.write.format("delta")
            .mode("append")
            .save(quarantine_path)
        )
        logger.warning(f"Quarantined {invalid_count:,} records to: {quarantine_path}")


def upsert_silver(
    spark: SparkSession,
    updates_df: DataFrame,
    silver_path: str,
    merge_key: str = "transaction_id",
) -> None:
    """
    Upsert (merge) incremental updates into silver Delta table.
    Uses Delta Lake MERGE for idempotent, SCD Type 1 behavior.
    """
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("target")
            .merge(
                updates_df.alias("source"),
                f"target.{merge_key} = source.{merge_key}",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"Upsert complete into: {silver_path}")
    else:
        updates_df.write.format("delta").mode("overwrite").save(silver_path)
        logger.info(f"Initial silver write to: {silver_path}")


if __name__ == "__main__":
    spark = create_spark_session()

    bronze_df = spark.read.format("delta").load("data/delta/bronze/transactions")
    silver_df = cleanse_transactions(bronze_df)

    validate_and_quarantine(
        df=silver_df,
        silver_path="data/delta/silver/transactions",
        quarantine_path="data/delta/quarantine/transactions",
    )

    spark.stop()
