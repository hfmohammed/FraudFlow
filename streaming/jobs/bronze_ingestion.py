"""
Bronze layer — Kafka → Delta Lake

The bronze table is a raw, immutable copy of every Kafka message.
No transformations happen here beyond JSON parsing. The principle: if silver or
gold have a bug, you can reprocess from bronze without re-reading Kafka
(which may have already expired messages under a short retention policy).
"""

import logging
import os
import sys

from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config
from spark_utils import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Explicit schema — never infer from data.
# Inferred schemas break silently when a new field is added or a type changes.
# An explicit schema surfaces those changes as a parse error you can handle intentionally.
TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("timestamp", StringType(), True),   # kept as string; silver parses to timestamp
        StructField("country", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_type", StringType(), True),
    ]
)


def main() -> None:
    config = load_config()
    spark = build_spark_session("FraudFlow-Bronze", include_kafka=True)

    logger.info("Bronze: reading from Kafka topic '%s' at %s",
                config.kafka_topic, config.kafka_bootstrap_servers)

    # Read from Kafka as a streaming source.
    # startingOffsets="earliest": on first run, consume all historical messages.
    # After that, the checkpoint stores the committed Kafka offset, so restart
    # picks up exactly where we left off — no messages are missed or re-processed.
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "earliest")
        # Cap the messages consumed per micro-batch.
        # Prevents a large Kafka backlog from making the first batch enormous
        # and causing out-of-memory or very slow first-batch processing.
        .option("maxOffsetsPerTrigger", 10_000)
        .load()
    )

    # from_json with an explicit schema: fields that don't match the schema come
    # back as null rather than crashing the job. Silver's validation step filters
    # those nulls out. We also retain Kafka's broker-side timestamp as an
    # independent timestamp for lag measurement: event_time vs kafka_ingest_time.
    parsed = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("d"),
            col("timestamp").alias("kafka_ingest_time"),
        )
        .select("d.*", "kafka_ingest_time")
        .withColumn("bronze_ingest_time", current_timestamp())
    )

    # WHY checkpointing:
    #   Spark stores two things in the checkpoint directory:
    #   (1) the last committed Kafka offset — so crash-recovery resumes mid-topic,
    #       not from the beginning;
    #   (2) streaming query metadata — so Spark can reconstruct query state.
    #   Without a checkpoint, every restart re-reads from "earliest" and
    #   re-lands all messages into bronze, creating duplicates.
    #
    # WHY append output mode:
    #   Bronze rows are written once and never updated or deleted.
    #   "append" is the only valid mode for insert-only streaming sinks —
    #   "update" and "complete" require aggregation operations.
    #
    # WHY processingTime="10 seconds":
    #   Bronze is the landing zone — low latency matters. 10-second micro-batches
    #   keep data fresh. On Databricks, use trigger(availableNow=True) to run
    #   bronze as a scheduled batch job that drains the Kafka backlog then exits.
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.bronze_checkpoint)
        .trigger(processingTime="10 seconds")
        .start(config.bronze_path)
    )

    logger.info("Bronze streaming query active. Writing to: %s", config.bronze_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
