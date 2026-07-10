import logging
import os

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

_DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"
_KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


def build_spark_session(app_name: str, include_kafka: bool = False) -> SparkSession:
    """
    Build a SparkSession with Delta Lake support, portable across local Docker
    and Databricks Community Edition.

    Portability strategy:
      - Locally: spark.jars.packages downloads Delta + Kafka JARs from Maven Central
        on first run (~1-2 min). Subsequent runs use the ~/.ivy2 cache.
      - On Databricks: DATABRICKS_RUNTIME_VERSION env var is set by the runtime.
        Delta is pre-installed; do NOT call configure_spark_with_delta_pip.
        Install the Kafka connector as a cluster library in the Databricks UI.
    """
    is_databricks = bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))

    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "1g")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    )

    if not is_databricks:
        packages = [_DELTA_PACKAGE]
        if include_kafka:
            packages.append(_KAFKA_PACKAGE)
        builder = builder.config("spark.jars.packages", ",".join(packages))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession '%s' ready (Databricks=%s)", app_name, is_databricks)
    return spark
