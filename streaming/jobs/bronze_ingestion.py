"""
Bronze layer: Kafka → Delta Lake

The bronze table is a raw, immutable copy of every Kafka message.
No transformations happen here beyond JSON parsing. The principle: if silver or
gold have a bug, you can reprocess from bronze without re-reading Kafka
(which may have already expired messages under a short retention policy).
"""

import logging
import os
import sys

from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config
from spark_utils import build_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("country", StringType(), True),
        StructField("is_fraud", BooleanType(), True),
        StructField("fraud_type", StringType(), True),
    ]
)


def main() -> None:
    config = load_config()
    spark = build_spark_session("FraudFlow-Bronze", include_kafka=True)

    logger.info("Bronze: reading from Kafka topic '%s' at %s",
                config.kafka_topic, config.kafka_bootstrap_servers)

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 10_000)
        .load()
    )

    parsed = (
        raw_stream
        .select(
            from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("d"),
            col("timestamp").alias("kafka_ingest_time"),
        )
        .select("d.*", "kafka_ingest_time")
        .withColumn("bronze_ingest_time", current_timestamp())
    )

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config.bronze_checkpoint)
        .trigger(processingTime="10 seconds")
        .start(config.bronze_path)
    )

    logger.info("Bronze streaming query active. Writing to: %s", config.bronze_path)
    query.awaitTermination()


if __name__ == "__main__":
    main()
