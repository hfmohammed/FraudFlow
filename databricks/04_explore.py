# Databricks notebook source
# MAGIC %md
# MAGIC # Exploratory Analysis : Gold Table
# MAGIC
# MAGIC Portfolio-ready queries and visualizations on the gold fraud signals table.
# MAGIC Use these as talking points when presenting the project.
# MAGIC
# MAGIC **Run order:** `00_setup` → `01_data_generator` → `02_silver` → `03_gold` → **`04_explore`**

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql.functions import col, count, avg, max as spark_max, round as spark_round, when, lit

gold   = spark.read.format("delta").load(GOLD_PATH)
silver = spark.read.format("delta").load(SILVER_PATH)
bronze = spark.read.format("delta").load(BRONZE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

b = bronze.count()
s = silver.count()
g = gold.count()
h = gold.filter(col("fraud_score") >= 0.70).count()

print(f"{'Layer':<10} {'Rows':>10}  {'Notes'}")
print(f"{'Bronze':<10} {b:>10,}  raw Kafka events (or generated)")
print(f"{'Silver':<10} {s:>10,}  deduplicated + validated  ({b-s:,} dropped)")
print(f"{'Gold':<10} {g:>10,}  enriched with fraud signals")
print(f"{'High conf':<10} {h:>10,}  fraud_score >= 0.70  ({100*h/g:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Breakdown by Type

# COMMAND ----------

display(
    gold.groupBy("fraud_type")
        .agg(
            count("*").alias("count"),
            spark_round(avg("fraud_score"), 3).alias("avg_score"),
            spark_round(avg("amount"), 2).alias("avg_amount"),
            spark_round(spark_max("amount"), 2).alias("max_amount"),
        )
        .orderBy("count", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top 20 Highest-Risk Cards

# COMMAND ----------

display(
    gold.filter(col("fraud_score") >= 0.70)
        .groupBy("card_id", "fraud_type")
        .agg(
            count("*").alias("flagged_events"),
            spark_round(spark_max("fraud_score"), 3).alias("peak_score"),
            spark_round(spark_max("amount"), 2).alias("max_amount"),
            spark_round(avg("velocity_60m"), 1).alias("avg_velocity"),
        )
        .orderBy("peak_score", ascending=False)
        .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Amount Z-Score Distribution
# MAGIC
# MAGIC Z-score > 3 means the transaction amount is more than 3 standard deviations
# MAGIC above the card's batch mean : a strong amount-spike signal.

# COMMAND ----------

display(
    gold.select(
        when(col("amount_zscore") < -1, "< -1")
        .when(col("amount_zscore") < 0,  "-1 to 0")
        .when(col("amount_zscore") < 1,  "0 to 1")
        .when(col("amount_zscore") < 2,  "1 to 2")
        .when(col("amount_zscore") < 3,  "2 to 3")
        .otherwise("> 3")
        .alias("zscore_bucket"),
        col("is_fraud")
    )
    .groupBy("zscore_bucket", "is_fraud")
    .agg(count("*").alias("count"))
    .orderBy("zscore_bucket")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fraud Rate by Country

# COMMAND ----------

display(
    gold.groupBy("country")
        .agg(
            count("*").alias("total_events"),
            count(when(col("is_fraud"), True)).alias("fraud_events"),
        )
        .withColumn("fraud_rate_pct",
                    spark_round(col("fraud_events") / col("total_events") * 100, 2))
        .orderBy("total_events", ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table History (Lineage)
# MAGIC
# MAGIC Delta Lake tracks every write operation : useful for debugging and auditing.

# COMMAND ----------

# MAGIC %python
# MAGIC display(spark.sql(f"DESCRIBE HISTORY delta.`{GOLD_PATH}`"))
