# Databricks notebook source
# MAGIC %md
# MAGIC # FraudFlow — Setup & Paths
# MAGIC
# MAGIC Run this cell first in every notebook via `%run ./00_setup`.
# MAGIC It sets the Delta table paths and validates the cluster environment.

# COMMAND ----------

# Detect the current Unity Catalog catalog automatically so this notebook works
# on Free Edition (UC-only) without manual path configuration.
# On CE/DBFS you can override by setting BASE_PATH manually before %run ./00_setup.
try:
    _catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{_catalog}`.fraudflow")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{_catalog}`.fraudflow.data")
    BASE_PATH = f"/Volumes/{_catalog}/fraudflow/data"
except Exception:
    # Fallback for CE / plain DBFS environments where dbfs:/ still works
    BASE_PATH = "dbfs:/fraudflow"

# DELTA_BASE_PATH is the only value that changes between environments:
#   Databricks Free Edition  →  /Volumes/<catalog>/fraudflow/data  (auto-detected above)
#   Databricks CE            →  "dbfs:/fraudflow"
#   Local Docker             →  "/data/fraudflow"
#   S3/EMR                   →  "s3://your-bucket/fraudflow"
#   Azure ADLS               →  "abfss://container@account.dfs.core.windows.net/fraudflow"

BRONZE_PATH = f"{BASE_PATH}/bronze/transactions"
SILVER_PATH = f"{BASE_PATH}/silver/transactions"
GOLD_PATH   = f"{BASE_PATH}/gold/fraud_signals"

BRONZE_CKPT = f"{BASE_PATH}/checkpoints/bronze"
SILVER_CKPT = f"{BASE_PATH}/checkpoints/silver"
GOLD_CKPT   = f"{BASE_PATH}/checkpoints/gold"

# COMMAND ----------

# On Databricks, `spark` is already available globally — no SparkSession.builder needed.
# Delta Lake is pre-installed in every Databricks Runtime >= 8.x.
# We only set shuffle partitions: CE clusters are single-node, so 8 is plenty.
spark.conf.set("spark.sql.shuffle.partitions", "8")

print(f"Spark:  {spark.version}")
print(f"Base:   {BASE_PATH}")
print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")
print(f"Gold:   {GOLD_PATH}")
