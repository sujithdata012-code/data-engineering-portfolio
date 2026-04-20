"""
Gold Layer — Business-Ready Aggregations
Reads from silver Delta tables and produces analyst-ready,
dimensional models for BI dashboards and regulatory reporting.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max,
    min as spark_min, date_trunc, countDistinct,
    current_timestamp, lit, when,
)
from delta.tables import DeltaTable
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("MedallionGoldAggregation")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def build_daily_transaction_summary(silver_df: DataFrame) -> DataFrame:
    """
    Aggregate daily transaction volumes and amounts per currency.
    Powers finance dashboards and daily risk reports.
    """
    logger.info("Building daily transaction summary...")

    return (
        silver_df
        .withColumn("transaction_date", date_trunc("day", col("transaction_ts")))
        .groupBy("transaction_date", "currency")
        .agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            spark_max("amount").alias("max_amount"),
            spark_min("amount").alias("min_amount"),
            countDistinct("customer_id").alias("unique_customers"),
        )
        .withColumn("_gold_timestamp", current_timestamp())
        .withColumn("_layer", lit("gold"))
        .orderBy("transaction_date", "currency")
    )


def build_customer_360(silver_df: DataFrame) -> DataFrame:
    """
    Customer-level lifetime aggregation.
    Powers customer risk scoring and segmentation models.
    """
    logger.info("Building customer 360 view...")

    return (
        silver_df
        .groupBy("customer_id")
        .agg(
            count("transaction_id").alias("total_transactions"),
            spark_sum("amount").alias("total_spend"),
            avg("amount").alias("avg_transaction_amount"),
            spark_max("transaction_ts").alias("last_transaction_ts"),
            spark_min("transaction_ts").alias("first_transaction_ts"),
            countDistinct("currency").alias("currencies_used"),
        )
        .withColumn("_gold_timestamp", current_timestamp())
        .withColumn("_layer", lit("gold"))
    )


def build_risk_flag_summary(
    silver_df: DataFrame,
    high_value_threshold: float = 10000.0
) -> DataFrame:
    """
    Flag high-value transactions for risk and compliance teams.
    Regulatory reporting layer.
    """
    logger.info(f"Building risk flags (threshold: ${high_value_threshold:,.2f})...")

    return (
        silver_df
        .withColumn(
            "risk_flag",
            when(col("amount") >= high_value_threshold, lit("HIGH_VALUE"))
            .when(col("amount") < 0, lit("NEGATIVE_AMOUNT"))
            .otherwise(lit("NORMAL"))
        )
        .groupBy("risk_flag", "currency")
        .agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
        )
        .withColumn("_gold_timestamp", current_timestamp())
    )


def write_gold_table(
    spark: SparkSession,
    df: DataFrame,
    gold_path: str,
    partition_col: str = None,
) -> None:
    """Write or overwrite a gold Delta table."""
    writer = (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
    )

    if partition_col:
        writer = writer.partitionBy(partition_col)

    writer.save(gold_path)
    logger.info(f"Gold table written: {gold_path} ({df.count():,} rows)")


if __name__ == "__main__":
    spark = create_spark_session()

    silver_df = spark.read.format("delta").load("data/delta/silver/transactions")

    daily_summary = build_daily_transaction_summary(silver_df)
    write_gold_table(spark, daily_summary, "data/delta/gold/daily_transaction_summary", "transaction_date")

    customer_360 = build_customer_360(silver_df)
    write_gold_table(spark, customer_360, "data/delta/gold/customer_360")

    risk_summary = build_risk_flag_summary(silver_df)
    write_gold_table(spark, risk_summary, "data/delta/gold/risk_flag_summary")

    spark.stop()
