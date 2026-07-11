# Databricks notebook source
# MAGIC %md
# MAGIC # FraudFlow Setup & Paths
# MAGIC
# MAGIC Run this cell first in every notebook via `%run ./00_setup`.
# MAGIC It sets the Delta table paths and validates the cluster environment.

# COMMAND ----------

try:
    _catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{_catalog}`.fraudflow")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{_catalog}`.fraudflow.data")
    BASE_PATH = f"/Volumes/{_catalog}/fraudflow/data"
except Exception:
    BASE_PATH = "dbfs:/fraudflow"

BRONZE_PATH = f"{BASE_PATH}/bronze/transactions"
SILVER_PATH = f"{BASE_PATH}/silver/transactions"
GOLD_PATH   = f"{BASE_PATH}/gold/fraud_signals"

BRONZE_CKPT = f"{BASE_PATH}/checkpoints/bronze"
SILVER_CKPT = f"{BASE_PATH}/checkpoints/silver"
GOLD_CKPT   = f"{BASE_PATH}/checkpoints/gold"

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "8")

print(f"Spark:  {spark.version}")
print(f"Base:   {BASE_PATH}")
print(f"Bronze: {BRONZE_PATH}")
print(f"Silver: {SILVER_PATH}")
print(f"Gold:   {GOLD_PATH}")
