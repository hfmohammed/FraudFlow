import logging
import os
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class Config:
    kafka_bootstrap_servers: str
    kafka_topic: str
    events_per_minute: int
    fraud_rate: float
    num_cards: int
    num_merchants: int
    metrics_port: int
    # Derived from events_per_minute — computed once here so the hot loop never divides.
    sleep_interval_seconds: float


def load_config() -> Config:
    events_per_minute = int(os.environ.get("EVENTS_PER_MINUTE", "1000"))
    fraud_rate = float(os.environ.get("FRAUD_RATE", "0.015"))

    if not (0.0 <= fraud_rate <= 0.5):
        raise ValueError(f"FRAUD_RATE must be between 0.0 and 0.5, got {fraud_rate}")

    if events_per_minute < 1 or events_per_minute > 50_000:
        raise ValueError(f"EVENTS_PER_MINUTE must be 1–50000, got {events_per_minute}")

    if events_per_minute > 10_000:
        logger.warning(
            "EVENTS_PER_MINUTE=%d: time.sleep() precision degrades above ~10k/min. "
            "Actual throughput may be lower than requested.",
            events_per_minute,
        )

    return Config(
        kafka_bootstrap_servers=os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"
        ),
        kafka_topic=os.environ.get("KAFKA_TOPIC", "transactions"),
        events_per_minute=events_per_minute,
        fraud_rate=fraud_rate,
        num_cards=int(os.environ.get("NUM_CARDS", "500")),
        num_merchants=int(os.environ.get("NUM_MERCHANTS", "200")),
        metrics_port=int(os.environ.get("METRICS_PORT", "8000")),
        sleep_interval_seconds=60.0 / events_per_minute,
    )
