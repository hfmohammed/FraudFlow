import os
from dataclasses import dataclass


@dataclass
class StreamingConfig:
    kafka_bootstrap_servers: str
    kafka_topic: str
    # DELTA_BASE_PATH is the only path that needs to change between environments:
    #   Local Docker:   /data/fraudflow          (bind-mounted volume)
    #   Databricks:     dbfs:/fraudflow          (DBFS) or abfss://... (ADLS)
    #   AWS EMR:        s3://your-bucket/fraudflow
    # All table and checkpoint paths derive from this single root.
    base_path: str

    @property
    def bronze_path(self) -> str:
        return f"{self.base_path}/bronze/transactions"

    @property
    def silver_path(self) -> str:
        return f"{self.base_path}/silver/transactions"

    @property
    def gold_path(self) -> str:
        return f"{self.base_path}/gold/fraud_signals"

    @property
    def bronze_checkpoint(self) -> str:
        return f"{self.base_path}/checkpoints/bronze"

    @property
    def silver_checkpoint(self) -> str:
        return f"{self.base_path}/checkpoints/silver"

    @property
    def gold_checkpoint(self) -> str:
        return f"{self.base_path}/checkpoints/gold"


def load_config() -> StreamingConfig:
    return StreamingConfig(
        kafka_bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"
        ),
        kafka_topic=os.environ.get("KAFKA_TOPIC", "transactions"),
        base_path=os.environ.get("DELTA_BASE_PATH", "/tmp/fraudflow"),
    )
