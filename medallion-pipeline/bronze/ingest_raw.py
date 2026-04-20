"""
Bronze Layer — Raw Ingestion
Ingests raw financial transaction data from source (CSV/JSON/API) into
Delta Lake bronze tables on ADLS Gen2. No transformations — raw as-is.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from delta import configure_spark_with_delta_pip
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "MedallionBronzeIngestion") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def ingest_transactions(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
    file_format: str = "csv",
) -> None:
    """
    Read raw transactions from source and write to bronze Delta table.
    Adds metadata columns: ingestion timestamp and source file name.
    """
    logger.info(f"Reading {file_format} data from: {source_path}")

    raw_df = (
        spark.read.format(file_format)
        .option("header", "true")
        .option("inferSchema", "true")
        .load(source_path)
    )

    # Add bronze metadata columns — never modify source data
    bronze_df = raw_df.withColumn(
        "_ingestion_timestamp", current_timestamp()
    ).withColumn(
        "_source_file", input_file_name()
    ).withColumn(
        "_layer", lit("bronze")
    )

    record_count = bronze_df.count()
    logger.info(f"Ingested {record_count:,} raw records")

    (
        bronze_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save(bronze_path)
    )

    logger.info(f"Bronze layer written to: {bronze_path}")


def ingest_market_data(
    spark: SparkSession,
    source_path: str,
    bronze_path: str,
) -> None:
    """Ingest raw market data (JSON format) into bronze."""
    logger.info(f"Reading market data from: {source_path}")

    raw_df = (
        spark.read.format("json")
        .option("multiLine", "true")
        .load(source_path)
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_layer", lit("bronze"))
    )

    (
        raw_df.write.format("delta")
        .mode("append")
        .partitionBy("date")
        .save(bronze_path)
    )

    logger.info(f"Market data bronze written to: {bronze_path}")


if __name__ == "__main__":
    spark = create_spark_session()

    ingest_transactions(
        spark=spark,
        source_path="data/sample/transactions.csv",
        bronze_path="data/delta/bronze/transactions",
    )

    ingest_market_data(
        spark=spark,
        source_path="data/sample/market_data.json",
        bronze_path="data/delta/bronze/market_data",
    )

    spark.stop()
