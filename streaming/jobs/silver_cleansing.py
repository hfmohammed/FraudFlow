"""
Silver layer — Bronze Delta → Silver Delta

Silver is the "trusted" layer: deduplicated, validated, properly typed.
Downstream analytics, ML feature engineering, and the gold aggregation job
all read from silver — never from bronze.

Reading from bronze (not Kafka) is the medallion architecture principle:
each layer only reads from the one below it. This means:
  - Silver can be rebuilt from scratch by replaying bronze, even after Kafka
    messages expire (Kafka retention is typically 7 days; bronze is forever).
  - The Kafka topic's retention policy is decoupled from silver's correctness.
  - If silver has a schema bug, you fix it and re-read bronze — no Kafka re-play needed.
"""

import logging
import os
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config
from spark_utils import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def wait_for_delta_table(spark: SparkSession, path: str, timeout: int = 180) -> None:
    """
    Block until the upstream Delta table exists and has at least one committed write.
    Silver and gold must wait for their source tables — trying to readStream from a
    path that has no Delta log yet raises DELTA_SCHEMA_NOT_SET immediately.
    Works for local paths, dbfs:/, s3://, and abfss:// without code changes.
    """
    from delta.tables import DeltaTable

    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            if DeltaTable.isDeltaTable(spark, path):
                logger.info("Upstream Delta table ready at %s", path)
                return
        except Exception:
            pass
        logger.info("Waiting for upstream Delta table at %s (bronze not ready yet)...", path)
        time.sleep(10)
    raise TimeoutError(f"Delta table at {path} did not appear within {timeout}s")


def main() -> None:
    config = load_config()
    spark = build_spark_session("FraudFlow-Silver")

    logger.info("Silver: reading from bronze at %s", config.bronze_path)

    wait_for_delta_table(spark, config.bronze_path)

    # Delta streaming source tracks which Delta files have been processed via the
    # transaction log. On each trigger it reads only new Delta files — never the
    # whole table. This is much more efficient than re-reading from Kafka.
    bronze_stream = spark.readStream.format("delta").load(config.bronze_path)

    cleaned = (
        bronze_stream
        # Parse the producer's ISO 8601 string into a native TimestampType.
        # We name it "event_time" to distinguish it from three other timestamps
        # that now travel with each row: kafka_ingest_time, bronze_ingest_time,
        # and silver_ingest_time (added below). Together they let you measure
        # Kafka lag, bronze processing lag, and silver processing lag independently.
        .withColumn("event_time", to_timestamp("timestamp"))

        # WHY withWatermark before dropDuplicates:
        #   dropDuplicates must remember which transaction_ids it has already seen
        #   to reject future duplicates. Without a watermark, that state grows
        #   unboundedly as the job runs — eventually exhausting driver memory.
        #   withWatermark("event_time", "10 minutes") tells Spark: "you can safely
        #   discard state for events whose event_time is more than 10 minutes behind
        #   the latest event time you've seen." Late arrivals beyond that window are
        #   not deduplicated (they pass through to silver as potential duplicates),
        #   but memory usage stays bounded. This is an explicit correctness trade-off:
        #   we sacrifice dedup guarantees for very late events in exchange for a
        #   streaming job that doesn't OOM after running for days.
        .withWatermark("event_time", "10 minutes")

        # Row-level validation. from_json returns null fields when the JSON doesn't
        # match the schema — these come from malformed producer messages or Kafka
        # message corruption. Filter them early so gold never sees bad rows.
        .filter(
            col("transaction_id").isNotNull()
            & col("card_id").isNotNull()
            & col("amount").isNotNull()
            & (col("amount") > 0)
            & col("event_time").isNotNull()
        )

        # Deduplicate by transaction_id within the watermark window.
        # Sources of duplicates in this pipeline:
        #   1. confluent-kafka producer retries on delivery timeout
        #   2. Kafka's at-least-once delivery guarantee (a consumer can receive a
        #      message twice if it crashes after processing but before committing)
        #   3. Bronze job restarted without a checkpoint (rare)
        # Including event_time in the dedup key keeps the watermark state bounded —
        # Spark only tracks (transaction_id, event_time) pairs within the watermark
        # window, not all transaction_ids ever seen.
        .dropDuplicates(["transaction_id", "event_time"])
    )

    query = (
        cleaned.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.silver_checkpoint)
        # 15s is slightly behind bronze (10s) — silver finishes after bronze writes,
        # so there's always data waiting for it. A longer trigger reduces overhead.
        .trigger(processingTime="15 seconds")
        .start(config.silver_path)
    )

    logger.info("Silver streaming query active. Writing to: %s", config.silver_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
