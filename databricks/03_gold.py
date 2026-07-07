# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Fraud Signals
# MAGIC
# MAGIC Reads silver, computes per-card fraud signals using `foreachBatch`, and
# MAGIC writes enriched rows to the gold Delta table.
# MAGIC
# MAGIC Signals per transaction:
# MAGIC | Signal | Description |
# MAGIC |--------|-------------|
# MAGIC | `velocity_60m` | Transactions from this card in the current micro-batch |
# MAGIC | `avg_amount_60m` | Mean amount for this card in the batch window |
# MAGIC | `amount_zscore` | Standard deviations from the card's batch mean |
# MAGIC | `geo_anomaly_flag` | True if `fraud_type == "impossible_travel"` |
# MAGIC | `fraud_score` | Weighted composite in [0, 1] |
# MAGIC
# MAGIC **Run order:** `00_setup` → `01_data_generator` → `02_silver` → **`03_gold`** → `04_explore`

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    abs as spark_abs, avg, col, count, current_timestamp,
    least, lit, stddev_samp, to_timestamp, when,
)
from pyspark.sql.types import DoubleType

ALERT_THRESHOLD = 0.70

# COMMAND ----------

def compute_fraud_signals(batch_df: DataFrame) -> DataFrame:
    """Enrich each row with per-card fraud signals for this micro-batch."""
    card_stats = batch_df.groupBy("card_id").agg(
        count("*").alias("velocity_60m"),
        avg("amount").alias("avg_amount_60m"),
        stddev_samp("amount").alias("_stddev"),
    )
    return (
        batch_df
        .join(card_stats, "card_id", "left")
        .withColumn(
            "amount_zscore",
            when(col("_stddev").isNotNull() & (col("_stddev") > 0),
                 (col("amount") - col("avg_amount_60m")) / col("_stddev"))
            .otherwise(lit(0.0))
            .cast(DoubleType())
        )
        .withColumn("velocity_flag",    col("velocity_60m") >= 8)
        .withColumn("geo_anomaly_flag", col("fraud_type") == lit("impossible_travel"))
        .withColumn(
            "fraud_score",
            (
                when(col("velocity_flag"), lit(0.40)).otherwise(lit(0.0))
                + least(spark_abs(col("amount_zscore")), lit(5.0)) / lit(5.0) * lit(0.40)
                + when(col("geo_anomaly_flag"), lit(0.20)).otherwise(lit(0.0))
            ).cast(DoubleType())
        )
        .withColumn("gold_processed_at", current_timestamp())
        .drop("_stddev")
    )

# COMMAND ----------

def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    enriched = compute_fraud_signals(batch_df)

    (enriched.write
             .format("delta")
             .mode("append")
             .save(GOLD_PATH))

    high = enriched.filter(col("fraud_score") >= ALERT_THRESHOLD).count()
    total = enriched.count()
    print(f"Batch {batch_id}: {total:,} rows | {high:,} high-confidence fraud "
          f"({100*high/total:.1f}%)")

# COMMAND ----------

silver_stream = (
    spark.readStream
    .format("delta")
    .load(SILVER_PATH)
    .withColumn("event_time", to_timestamp("timestamp"))
)

# WHY foreachBatch:
#   Gives you the full batch DataFrame API inside each micro-batch — multi-step
#   joins, external API calls (EventBridge), complex scoring logic. The trade-off
#   is losing Spark's built-in state management for windowed aggregations. For
#   gold-layer business logic that needs all of the above, foreachBatch is the
#   standard pattern in production Spark Streaming codebases.
query = (
    silver_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", GOLD_CKPT)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

# COMMAND ----------

gold_df = spark.read.format("delta").load(GOLD_PATH)
total   = gold_df.count()
high    = gold_df.filter(col("fraud_score") >= ALERT_THRESHOLD).count()

print(f"Gold table: {total:,} rows | {high:,} high-confidence fraud ({100*high/total:.1f}%)")

display(
    gold_df
    .orderBy(col("fraud_score").desc())
    .select("card_id", "fraud_type", "amount", "velocity_60m",
            "amount_zscore", "fraud_score", "country", "timestamp")
    .limit(20)
)
