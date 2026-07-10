# Running FraudFlow on Databricks Community Edition

## Prerequisites

1. Free account at [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. This repo cloned locally (notebooks are `.py` files in `databricks/`)

---

## 1. Create a Cluster

**Compute → Create compute**

| Setting | Value |
|---|---|
| Policy | Unrestricted |
| Single node | ✓ (CE only has single-node) |
| Databricks Runtime | **14.3 LTS** (Spark 3.5, Delta 3.1, Python 3.11) |
| Node type | default (CE assigns it) |

> Runtime 14.3 LTS ships with Delta Lake pre-installed : no pip installs needed
> for Spark or Delta. The notebooks are self-contained.

---

## 2. Import Notebooks

**Workspace → your username folder → Import**

Import each `.py` file from `databricks/` as a **Source file**. Databricks
recognises the `# Databricks notebook source` header and renders them as notebooks.

Import in this order (order only matters for display; execution order is set by `%run`):

```
00_setup.py
01_data_generator.py
02_silver.py
03_gold.py
04_explore.py
```

---

## 3. Run in Order

Attach all notebooks to the cluster you created, then run them top-to-bottom:

```
00_setup            → prints paths, confirms Spark version
01_data_generator   → generates 50k synthetic transactions → writes to bronze Delta
02_silver           → dedup + validate bronze → writes to silver Delta
03_gold             → fraud signals + scoring → writes to gold Delta
04_explore          → queries + visualizations on gold table
```

Each notebook starts with `%run ./00_setup` so path variables are always in scope.

---

## 4. Verify Data on DBFS

After running `01_data_generator`, confirm bronze is written:

```python
display(dbutils.fs.ls("dbfs:/fraudflow/bronze/transactions"))
```

You should see Delta files (`part-*.parquet`) and a `_delta_log/` directory.

---

## 5. Key Differences from Local Docker

| Aspect | Local Docker | Databricks CE |
|---|---|---|
| Kafka source | `spark.readStream.format("kafka")` | Not available : replaced by `01_data_generator.py` |
| Trigger | `processingTime="10 seconds"` | `availableNow=True` (run once, finish) |
| SparkSession | `SparkSession.builder...getOrCreate()` | `spark` is pre-injected globally |
| Delta setup | `configure_spark_with_delta_pip` | Pre-installed in runtime |
| Storage path | `/data/fraudflow` (Docker volume) | `dbfs:/fraudflow` |
| Continuous streaming | Containers run forever | Re-run cells or schedule with Databricks Jobs |

The silver and gold **logic is identical** : only the trigger and session setup differ.

---

## 6. Production Databricks (non-CE)

On a full Databricks workspace with Confluent Cloud or Amazon MSK:

1. Replace `01_data_generator.py` with `streaming/jobs/bronze_ingestion.py`
2. Point `KAFKA_BOOTSTRAP_SERVERS` at your cloud Kafka endpoint
3. Change `trigger(availableNow=True)` back to `trigger(processingTime="...")` in silver/gold
4. Use Databricks Jobs to run each notebook as a continuous streaming task

`DELTA_BASE_PATH` is the only path change needed (`dbfs:/fraudflow` → `abfss://...` or `s3://...`).
