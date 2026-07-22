"""
Silver layer: Bronze Delta → Silver Delta

Silver is the "trusted" layer: deduplicated, validated, properly typed.
Downstream analytics, ML feature engineering, and the gold aggregation job
all read from silver, never from bronze.

Reading from bronze (not Kafka) is the medallion architecture principle:
each layer only reads from the one below it. This means:
  - Silver can be rebuilt from scratch by replaying bronze, even after Kafka
    messages expire (Kafka retention is typically 7 days; bronze is forever).
  - The Kafka topic's retention policy is decoupled from silver's correctness.
  - If silver has a schema bug, you fix it and re-read bronze; no Kafka re-play needed.
"""

import logging

from pyspark.sql.functions import col, to_timestamp

from streaming.config import load_config
from streaming.spark_utils import build_spark_session, wait_for_delta_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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
