"""
FraudFlow transaction producer.

Generates simulated card transactions and publishes them to a Kafka topic.
Uses confluent-kafka (backed by librdkafka in C) rather than kafka-python because
librdkafka handles batching and network I/O in C threads that run outside Python's
GIL — essential for sustaining 10k–50k events/minute without GIL contention.

NOTE on serialization: events are JSON-encoded strings, not Avro/Protobuf.
JSON requires no schema registry, is human-readable, and Spark can parse it with
`from_json()`. In production you would use Confluent Schema Registry + Avro to
enforce schema contracts and enable schema evolution without redeployment.
"""

import json
import logging
import signal
import time
from collections import deque

from confluent_kafka import Producer, KafkaException
from prometheus_client import Counter, Gauge, start_http_server

from config import load_config
from fraud_patterns import FraudInjector
from transaction_generator import TransactionGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------
events_counter = Counter(
    "producer_events_total",
    "Total events published, labelled by fraud status",
    ["fraud"],
)
events_per_second_gauge = Gauge(
    "producer_events_per_second",
    "Rolling 5-second average production rate",
)
kafka_errors_counter = Counter(
    "producer_kafka_send_errors_total",
    "Total Kafka delivery errors reported by librdkafka",
)

# ---------------------------------------------------------------------------
# Delivery callback — called by confluent-kafka when a message is acked or fails.
# IMPORTANT: this is only invoked when producer.poll() is called. Without poll(),
# you never see errors and your error counter always stays at 0.
# ---------------------------------------------------------------------------

def _delivery_callback(err, msg):
    if err is not None:
        kafka_errors_counter.inc()
        logger.warning("Delivery failed for key=%s: %s", msg.key(), err)


# ---------------------------------------------------------------------------
# Kafka producer with retry on startup
# ---------------------------------------------------------------------------

def _build_producer(bootstrap_servers: str) -> Producer:
    conf = {
        "bootstrap.servers": bootstrap_servers,
        # Wait up to 5ms to batch messages before sending.
        # At 1k events/min this barely matters; at 50k/min it cuts network round-trips.
        "linger.ms": 5,
        "batch.num.messages": 1000,
        # LZ4 compresses JSON well and is fast on both ends vs gzip.
        "compression.type": "lz4",
        # Leader ack only. All-replicas ack (acks=all) would be more durable but adds
        # latency and requires a multi-broker setup. Fine for a single-node dev cluster.
        "acks": "1",
    }
    # Kafka may not be fully ready even after the Docker health check passes — retry.
    for attempt in range(1, 4):
        try:
            producer = Producer(conf)
            # A metadata fetch is the cheapest way to confirm the broker is reachable.
            producer.list_topics(timeout=5)
            logger.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except KafkaException as exc:
            wait = 2 ** attempt  # 2s, 4s, 8s
            logger.warning(
                "Kafka not ready (attempt %d/3): %s — retrying in %ds", attempt, exc, wait
            )
            time.sleep(wait)
    raise RuntimeError(f"Could not connect to Kafka at {bootstrap_servers} after 3 attempts")


# ---------------------------------------------------------------------------
# Main event loop
# ---------------------------------------------------------------------------

def run() -> None:
    config = load_config()

    logger.info(
        "Starting producer: %d events/min, fraud_rate=%.1f%%, %d cards, %d merchants",
        config.events_per_minute,
        config.fraud_rate * 100,
        config.num_cards,
        config.num_merchants,
    )

    # Expose /metrics endpoint for Prometheus scraping (non-blocking HTTP server).
    start_http_server(config.metrics_port)
    logger.info("Prometheus metrics available at http://0.0.0.0:%d/metrics", config.metrics_port)

    producer = _build_producer(config.kafka_bootstrap_servers)
    generator = TransactionGenerator(config)
    injector = FraudInjector(config.fraud_rate)

    # Rolling window for the events_per_second gauge: track timestamps of the last
    # events over a 5-second window, then divide by 5.
    recent_timestamps: deque = deque()
    last_gauge_update = time.monotonic()

    # Graceful shutdown: SIGTERM sets this flag; the loop drains before exiting.
    running = True

    def _handle_sigterm(signum, frame):
        nonlocal running
        logger.info("SIGTERM received — draining producer queue...")
        running = False

    signal.signal(signal.SIGTERM, _handle_sigterm)

    logger.info("Publishing to topic '%s'...", config.kafka_topic)

    while running:
        loop_start = time.monotonic()

        card_id = generator.pick_card_id()
        card = generator.card_profiles[card_id]

        fraud_decision = injector.decide(card)
        txn = generator.generate_transaction(card_id, fraud_decision)
        payload = json.dumps(txn).encode("utf-8")

        try:
            producer.produce(
                topic=config.kafka_topic,
                # Partition by card_id so all events from one card go to the same
                # partition — preserves ordering per card, which Spark streaming relies on.
                key=card_id.encode("utf-8"),
                value=payload,
                on_delivery=_delivery_callback,
            )
            # poll(0) is non-blocking but drains the delivery report queue so that
            # _delivery_callback fires promptly. Without this, callbacks may never run.
            producer.poll(0)
        except BufferError:
            # librdkafka's internal queue is full — Kafka is falling behind.
            kafka_errors_counter.inc()
            logger.warning("Producer queue full — Kafka is slow. Backing off 1s.")
            producer.poll(1)

        # Update Prometheus counters.
        fraud_label = "true" if txn["is_fraud"] else "false"
        events_counter.labels(fraud=fraud_label).inc()

        # Update rolling events/second gauge every second.
        now_mono = time.monotonic()
        recent_timestamps.append(now_mono)
        if now_mono - last_gauge_update >= 1.0:
            cutoff = now_mono - 5.0
            while recent_timestamps and recent_timestamps[0] < cutoff:
                recent_timestamps.popleft()
            events_per_second_gauge.set(len(recent_timestamps) / 5.0)
            last_gauge_update = now_mono

        # Rate limiting: sleep for the remainder of the target interval.
        # time.sleep() has ~1ms OS granularity; at >10k events/min the sleep_interval
        # is <6ms and precision degrades. For those rates a token-bucket approach
        # would give smoother throughput, but at the default 1k/min (60ms sleep) this is fine.
        elapsed = time.monotonic() - loop_start
        sleep_time = config.sleep_interval_seconds - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    # Flush remaining messages before exiting (up to 10s).
    flushed = producer.flush(timeout=10)
    if flushed > 0:
        logger.warning("%d messages were NOT delivered before shutdown", flushed)
    logger.info("Producer shut down cleanly.")


if __name__ == "__main__":
    run()
