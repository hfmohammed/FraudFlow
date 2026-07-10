# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer : Dedup + Validation
# MAGIC
# MAGIC Reads the bronze Delta table, deduplicates by `transaction_id`, validates
# MAGIC required fields, and writes clean rows to the silver Delta table.
# MAGIC
# MAGIC **Key difference from local Docker:**
# MAGIC `trigger(availableNow=True)` replaces `trigger(processingTime="15 seconds")`.
# MAGIC `availableNow` processes all data currently in the source table, then stops :
# MAGIC perfect for running in a notebook cell. On local Docker the job runs forever.
# MAGIC
# MAGIC **Run order:** `00_setup` → `01_data_generator` → **`02_silver`** → `03_gold` → `04_explore`

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

cleaned = (
    bronze_stream
    .withColumn("event_time", to_timestamp("timestamp"))
    .withWatermark("event_time", "10 minutes")

    .filter(
        col("transaction_id").isNotNull()
        & col("card_id").isNotNull()
        & (col("amount") > 0)
        & col("event_time").isNotNull()
    )

    .dropDuplicates(["transaction_id", "event_time"])
)

# COMMAND ----------

query = (
    cleaned.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SILVER_CKPT)
    .trigger(availableNow=True)
    .start(SILVER_PATH)
)

query.awaitTermination()

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_PATH)
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

bronze_count = bronze_df.count()
silver_count = silver_df.count()
dropped      = bronze_count - silver_count

print(f"Bronze rows:  {bronze_count:,}")
print(f"Silver rows:  {silver_count:,}  ({dropped:,} dropped as duplicates/invalid)")

display(silver_df.orderBy("event_time", ascending=False).limit(10))
