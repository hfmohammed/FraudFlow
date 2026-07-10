"""
Silver layer : Bronze Delta → Silver Delta

Silver is the "trusted" layer: deduplicated, validated, properly typed.
Downstream analytics, ML feature engineering, and the gold aggregation job
all read from silver : never from bronze.

Reading from bronze (not Kafka) is the medallion architecture principle:
each layer only reads from the one below it. This means:
  - Silver can be rebuilt from scratch by replaying bronze, even after Kafka
    messages expire (Kafka retention is typically 7 days; bronze is forever).
  - The Kafka topic's retention policy is decoupled from silver's correctness.
  - If silver has a schema bug, you fix it and re-read bronze : no Kafka re-play needed.
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
    Silver and gold must wait for their source tables : trying to readStream from a
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

    bronze_stream = spark.readStream.format("delta").load(config.bronze_path)

    cleaned = (
        bronze_stream
        .withColumn("event_time", to_timestamp("timestamp"))
        .withWatermark("event_time", "10 minutes")
        .filter(
            col("transaction_id").isNotNull()
            & col("card_id").isNotNull()
            & col("amount").isNotNull()
            & (col("amount") > 0)
            & col("event_time").isNotNull()
        )
        .dropDuplicates(["transaction_id", "event_time"])
    )

    query = (
        cleaned.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.silver_checkpoint)
        .trigger(processingTime="15 seconds")
        .start(config.silver_path)
    )

    logger.info("Silver streaming query active. Writing to: %s", config.silver_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
