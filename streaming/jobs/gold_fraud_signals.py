"""
Gold layer — Silver Delta → Fraud Signals Delta

Gold computes per-transaction fraud signals and a composite fraud score.
It uses foreachBatch, which turns each micro-batch into a regular Spark DataFrame.

WHY foreachBatch instead of a native streaming aggregation:
  Native streaming aggregations (groupBy + window) are elegant but constrained:
  you can only use "update" or "complete" output modes, and complex multi-step
  logic (e.g. computing a score from multiple signals then calling an external API)
  doesn't fit cleanly into a single operator. foreachBatch gives you the full
  batch DataFrame API inside each micro-batch — joins, multi-step transforms,
  external calls — at the cost of losing Spark's built-in state management.
  For gold-layer business logic that needs all of the above, foreachBatch is
  the standard pattern you'll see in every production Spark Streaming codebase.

Signals computed per transaction:
  velocity_60m    — transactions from this card in the current micro-batch
                    (proxy for sustained high frequency across the window)
  avg_amount_60m  — mean amount for this card in the micro-batch window
  amount_zscore   — standard deviations from the card's batch mean (batch-relative)
  geo_anomaly     — True if fraud_type == "impossible_travel" (producer-labeled)
  fraud_score     — weighted composite in [0, 1]
                    velocity (40%) + amount spike (40%) + geo anomaly (20%)
"""

import json
import logging
import os
import sys
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    current_timestamp,
    least,
    lit,
    stddev_samp,
    to_timestamp,
    when,
)
from pyspark.sql.types import DoubleType

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config, StreamingConfig
from spark_utils import build_spark_session


def wait_for_delta_table(spark: SparkSession, path: str, timeout: int = 180) -> None:
    """Block until the upstream Delta table has at least one committed write."""
    from delta.tables import DeltaTable

    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            if DeltaTable.isDeltaTable(spark, path):
                logger.info("Upstream Delta table ready at %s", path)
                return
        except Exception:
            pass
        logger.info("Waiting for upstream Delta table at %s (silver not ready yet)...", path)
        time.sleep(10)
    raise TimeoutError(f"Delta table at {path} did not appear within {timeout}s")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Publish to the fraud-alerts Kafka topic when fraud_score exceeds this threshold.
# 0.7 means at least two strong signals fired simultaneously — high confidence.
ALERT_THRESHOLD = 0.70


# ---------------------------------------------------------------------------
# Fraud signal computation
# ---------------------------------------------------------------------------

def compute_fraud_signals(batch_df: DataFrame) -> DataFrame:
    """Enrich each row with per-card fraud signals computed within the micro-batch."""

    # Per-card statistics for this micro-batch window.
    # These are batch-relative stats — they catch within-window anomalies.
    # Example: CARD-0042 sends 12 transactions in one 30-second batch with varying
    # amounts. The stddev of those amounts feeds the z-score computation.
    card_stats = batch_df.groupBy("card_id").agg(
        count("*").alias("velocity_60m"),
        avg("amount").alias("avg_amount_60m"),
        stddev_samp("amount").alias("_stddev_amount"),
    )

    return (
        batch_df
        .join(card_stats, "card_id", "left")

        # Amount z-score: how many standard deviations this transaction's amount
        # is above the card's batch mean. Capped at 5 so a single extreme outlier
        # doesn't dominate the composite score. Set to 0 when stddev is null
        # (card has only one transaction in this batch — z-score is undefined).
        .withColumn(
            "amount_zscore",
            when(
                col("_stddev_amount").isNotNull() & (col("_stddev_amount") > 0),
                (col("amount") - col("avg_amount_60m")) / col("_stddev_amount"),
            )
            .otherwise(lit(0.0))
            .cast(DoubleType()),
        )

        # Velocity flag: 8+ transactions from the same card in a single 30-second
        # micro-batch = 16+ per minute. Normal cards average ~2 per hour.
        .withColumn("velocity_flag", col("velocity_60m") >= 8)

        # Geo anomaly: the producer already labeled impossible-travel events.
        # We surface this as a boolean so downstream queries don't need to know
        # about the internal fraud_type taxonomy.
        .withColumn(
            "geo_anomaly_flag",
            col("fraud_type") == lit("impossible_travel"),
        )

        # Composite fraud score in [0, 1]. Three independent signals:
        #   velocity  (40%): binary flag — 8+ txns in window
        #   amount    (40%): normalized capped z-score → [0, 1]
        #   geo       (20%): binary flag — impossible travel
        # A score > 0.7 means at least two strong signals fired simultaneously.
        .withColumn(
            "fraud_score",
            (
                when(col("velocity_flag"), lit(0.40)).otherwise(lit(0.0))
                + least(spark_abs(col("amount_zscore")), lit(5.0)) / lit(5.0) * lit(0.40)
                + when(col("geo_anomaly_flag"), lit(0.20)).otherwise(lit(0.0))
            ).cast(DoubleType()),
        )

        .withColumn("gold_processed_at", current_timestamp())
        .drop("_stddev_amount")
    )


# ---------------------------------------------------------------------------
# Kafka alerting
# ---------------------------------------------------------------------------

def publish_alerts(batch_df: DataFrame, alerts_topic: str, bootstrap_servers: str) -> None:
    """
    Publish high-confidence fraud events to the fraud-alerts Kafka topic.

    Runs on the Spark driver — confluent-kafka Producer is not serializable
    so it cannot run on executors. We .collect() only high-score rows
    (typically <1% of traffic), so the driver-side collect is safe.
    Any downstream system (notifications service, compliance dashboard, etc.)
    subscribes to the topic independently — detection and notification are decoupled.
    """
    try:
        from confluent_kafka import Producer

        high_confidence = (
            batch_df
            .filter(col("fraud_score") >= ALERT_THRESHOLD)
            .select(
                "transaction_id", "card_id", "fraud_type", "amount",
                "merchant_category", "country", "timestamp", "fraud_score",
            )
            .collect()
        )

        if not high_confidence:
            return

        producer = Producer({"bootstrap.servers": bootstrap_servers})

        for row in high_confidence:
            payload = json.dumps({
                "transaction_id": row["transaction_id"],
                "card_id": row["card_id"],
                "fraud_type": row["fraud_type"] or "unknown",
                "amount": float(row["amount"]),
                "merchant_category": row["merchant_category"],
                "country": row["country"],
                "timestamp": row["timestamp"],
                "confidence_score": float(row["fraud_score"]),
            }).encode("utf-8")

            producer.produce(
                topic=alerts_topic,
                key=row["transaction_id"].encode("utf-8"),
                value=payload,
            )

        producer.flush()
        logger.info("Published %d fraud alerts to Kafka topic '%s'", len(high_confidence), alerts_topic)

    except Exception:
        # Never let alerting failures crash the streaming job.
        logger.exception("Kafka alert publish failed — continuing without alerting")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    config = load_config()
    spark = build_spark_session("FraudFlow-Gold")
    alerts_topic = config.alerts_topic
    bootstrap_servers = config.kafka_bootstrap_servers

    logger.info("Gold: reading from silver at %s", config.silver_path)

    wait_for_delta_table(spark, config.silver_path)

    silver_stream = (
        spark.readStream
        .format("delta")
        .load(config.silver_path)
        .withColumn("event_time", to_timestamp("timestamp"))
    )

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        row_count = batch_df.count()
        logger.info("Gold batch %d: %d rows", batch_id, row_count)

        if row_count == 0:
            return

        enriched = compute_fraud_signals(batch_df)

        # Publish high-confidence events to Kafka before writing to Delta,
        # so an alert is sent even if the Delta write later fails.
        publish_alerts(enriched, alerts_topic, bootstrap_servers)

        # Append to gold Delta table.
        # WHY append (not merge/upsert):
        #   Each row is a point-in-time snapshot of a transaction's fraud signals.
        #   Appending preserves the full history — if you retrain the scoring model
        #   and reprocess silver, you can compare old and new scores side-by-side.
        #   For a "current score" view, create a Delta view or downstream query
        #   that picks the latest row per transaction_id.
        (
            enriched.write
            .format("delta")
            .mode("append")
            .save(config.gold_path)
        )

        # Log the fraud detection rate for this batch.
        fraud_count = enriched.filter(col("fraud_score") >= ALERT_THRESHOLD).count()
        logger.info(
            "Gold batch %d complete: %d high-confidence fraud events (%.1f%%)",
            batch_id,
            fraud_count,
            100.0 * fraud_count / row_count if row_count else 0,
        )

    query = (
        silver_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", config.gold_checkpoint)
        # 30s trigger: gold is the slowest layer — it reads silver (15s trigger)
        # and performs joins + external API calls. A longer trigger amortizes that overhead.
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info("Gold streaming query active.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
