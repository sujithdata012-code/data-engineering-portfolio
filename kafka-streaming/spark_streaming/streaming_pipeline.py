"""
Spark Structured Streaming — Kafka → Delta Lake
Consumes financial transaction events from Kafka at ~5K events/sec,
applies transformations and risk flagging, lands into Delta tables
with checkpointing and fault tolerance.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, when, lit,
    window, sum as spark_sum, count,
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType,
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "financial-transactions"
DELTA_OUTPUT_PATH = "data/delta/streaming/transactions"
CHECKPOINT_PATH = "data/checkpoints/transactions"
AGGREGATES_PATH = "data/delta/streaming/windowed_aggregates"
AGGREGATES_CHECKPOINT = "data/checkpoints/aggregates"


TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("is_high_value", BooleanType(), True),
    StructField("status", StringType(), True),
    StructField("event_timestamp", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("KafkaToDeltaLakeStreaming")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    """Read raw events from Kafka topic as a streaming DataFrame."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10000)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_enrich(raw_stream):
    """
    Parse JSON payload, enrich with risk flags and processing metadata.
    """
    return (
        raw_stream
        .select(
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select("data.*", "kafka_timestamp")
        .withColumn("processing_timestamp", current_timestamp())

        # Risk classification
        .withColumn(
            "risk_tier",
            when(col("amount") >= 10000, lit("HIGH"))
            .when(col("amount") >= 1000, lit("MEDIUM"))
            .otherwise(lit("LOW"))
        )

        # Late arrival detection
        .withColumn(
            "is_late_arrival",
            when(
                col("kafka_timestamp").cast("long") -
                col("processing_timestamp").cast("long") > 300,
                lit(True)
            ).otherwise(lit(False))
        )

        # Filter malformed records
        .filter(col("transaction_id").isNotNull())
        .filter(col("amount") > 0)
    )


def write_to_delta(stream, output_path: str, checkpoint_path: str):
    """Write enriched stream to Delta Lake with checkpointing."""
    return (
        stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .trigger(processingTime="2 seconds")
        .start(output_path)
    )


def write_windowed_aggregates(stream, output_path: str, checkpoint_path: str):
    """
    5-minute tumbling window aggregates by currency.
    Near real-time risk monitoring feed.
    """
    windowed = (
        stream
        .withWatermark("processing_timestamp
