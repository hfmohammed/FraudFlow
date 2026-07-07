# FraudFlow

Real-time payments fraud detection pipeline — a data engineering portfolio project.

## Architecture

```mermaid
flowchart LR
    subgraph local["Local Docker Compose"]
        P["Python Producer\n(confluent-kafka)"]
        K["Kafka\n(KRaft, no ZK)"]
        SP["PySpark\nStructured Streaming\n(Phase 2)"]
        B[("Bronze\nDelta Table")]
        S[("Silver\nDelta Table")]
        G[("Gold\nDelta Table")]
        PR["Prometheus"]
        GR["Grafana"]
        P -- "JSON events\n1k–50k/min" --> K
        K --> SP
        SP --> B
        B --> S
        S --> G
        P -- "/metrics" --> PR
        PR --> GR
    end

    subgraph aws["AWS (alerting)"]
        EB["EventBridge\nFraudflowBus"]
        L["Lambda\nFraudAlertFunction"]
        SES["SES\nemail alert"]
        G -- "put-events\n(fraud rows)" --> EB
        EB -- "FraudDetected rule" --> L
        L --> SES
    end
```

**Stack:** Kafka (KRaft), PySpark Structured Streaming, Delta Lake (bronze/silver/gold medallion), Prometheus, Grafana, AWS Lambda + EventBridge + SES

---

## Quickstart

### Prerequisites
- Docker Desktop ≥ 4.x with Compose v2
- `kcat` (optional, for consuming events from the host): `brew install kcat`

### 1. Start the infrastructure

```bash
docker compose up -d
```

This starts Kafka, Prometheus, and Grafana. The producer is excluded by default.

```bash
# Verify Kafka is healthy (~30s after first boot)
docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 2. Start the producer

```bash
docker compose --profile producer up -d producer
docker compose logs -f producer
```

You should see: `Publishing to topic 'transactions'...`

### 3. Verify events are flowing

```bash
# From the host (kcat):
kcat -b localhost:9094 -t transactions -C -o beginning | head -5

# Or via Docker:
docker compose exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic transactions \
  --from-beginning \
  --max-messages 5
```

Each event looks like:
```json
{
  "transaction_id": "3f7a...",
  "card_id": "CARD-0042",
  "amount": 47.83,
  "merchant_id": "MERCH-0117",
  "merchant_category": "grocery",
  "lat": 40.71,
  "lon": -74.01,
  "timestamp": "2024-01-15T14:23:45Z",
  "country": "US",
  "is_fraud": false,
  "fraud_type": null
}
```

### 4. Monitoring

| Service | URL | Credentials |
|---|---|---|
| Grafana | http://localhost:3000 | admin / fraudflow |
| Prometheus | http://localhost:9090 | — |
| Producer metrics | http://localhost:8000/metrics | — |

Prometheus targets page: http://localhost:9090/targets — `producer:8000` should show **UP**.

### 5. Tune the producer

Override via environment variables in `.env` or pass directly:

```bash
EVENTS_PER_MINUTE=5000 FRAUD_RATE=0.02 docker compose --profile producer up producer
```

| Variable | Default | Range |
|---|---|---|
| `EVENTS_PER_MINUTE` | 1000 | 1 – 50000 |
| `FRAUD_RATE` | 0.015 | 0.0 – 0.5 |
| `NUM_CARDS` | 500 | — |
| `NUM_MERCHANTS` | 200 | — |

### 6. Stop everything

```bash
docker compose --profile producer down
```

---

## Fraud Patterns

The producer injects three fraud patterns at the configured `FRAUD_RATE` (~1.5% by default):

| Pattern | `fraud_type` | Signal |
|---|---|---|
| Amount spike | `amount_spike` | A card that normally spends $10–$80 submits a $500–$3000 transaction |
| Velocity burst | `velocity_burst` | Same card fires 8–15 transactions in a 60-second window |
| Impossible travel | `impossible_travel` | Same card used >2,000 km apart within 5 minutes |

Each event carries `is_fraud: true/false` and `fraud_type: <string or null>` as ground-truth labels for downstream ML.

---

## AWS Alerting (Lambda + EventBridge + SES)

When the Spark gold job (Phase 2) detects fraud, it publishes to an EventBridge custom bus. A Lambda function receives the event and sends an HTML email via SES.

### Prerequisites
1. `aws configure` with an IAM user that has AdministratorAccess
2. Verify both sender and recipient email in **SES console → Verified Identities** (required in SES sandbox)
3. `brew install aws-sam-cli` and confirm with `sam --version`

### Deploy

```bash
cd alerting
sam build
sam deploy --guided   # first time only — creates samconfig.toml (gitignored)
```

Pass `--parameter-overrides SenderEmail=you@example.com RecipientEmail=you@example.com` on subsequent deploys.

### Test without Spark

```bash
# Test the Lambda locally (SES call will fail without real AWS creds)
sam local invoke FraudAlertFunction \
  --event events/test_fraud_event.json \
  --env-vars '{"FraudAlertFunction":{"SENDER_EMAIL":"you@example.com","RECIPIENT_EMAIL":"you@example.com","AWS_REGION_OVERRIDE":"us-east-1"}}'

# Publish a real test event to the deployed EventBridge bus
aws events put-events \
  --entries '[{"Source":"fraudflow.gold","DetailType":"FraudDetected","EventBusName":"FraudflowBus","Detail":"{\"transaction_id\":\"TXN-001\",\"card_id\":\"CARD-0042\",\"fraud_type\":\"velocity_burst\",\"amount\":127.50,\"merchant_category\":\"online\",\"country\":\"US\",\"timestamp\":\"2024-01-15T14:23:45Z\",\"confidence_score\":0.87}"}]'

# Watch Lambda logs in real time
aws logs tail /aws/lambda/FraudAlertFunction --follow
```

> **Common gotcha:** The `Detail` field in `put-events` must be a JSON-encoded *string* (i.e. `json.dumps(dict)`), not a nested JSON object. Passing a nested object is silently accepted but EventBridge rule matching will fail.

---

## Project Structure

```
FraudFlow/
├── docker-compose.yml          # Kafka (KRaft), Prometheus, Grafana, producer
├── .env                        # Default env vars (committed, no secrets)
├── producer/
│   ├── producer.py             # Main event loop + Prometheus metrics
│   ├── transaction_generator.py# Card/merchant state, event schema
│   ├── fraud_patterns.py       # AmountSpike, VelocityBurst, ImpossibleTravel
│   ├── config.py               # All env var parsing in one place
│   ├── Dockerfile
│   └── requirements.txt
├── monitoring/
│   ├── prometheus.yml
│   └── grafana/provisioning/datasources/prometheus.yml
├── alerting/
│   ├── template.yaml           # SAM: EventBridge bus + rule + Lambda + IAM
│   ├── fraud_alert/
│   │   ├── handler.py          # Lambda entry point
│   │   └── email_formatter.py  # HTML + plain-text email builder
│   ├── events/
│   │   └── test_fraud_event.json
│   └── requirements.txt
├── streaming/                  # PySpark Structured Streaming jobs
│   ├── spark_utils.py          # SparkSession factory (local + Databricks portable)
│   ├── Dockerfile
│   └── jobs/
│       ├── bronze_ingestion.py # Kafka → Delta (raw, immutable)
│       ├── silver_cleansing.py # Dedup (watermark) + validation
│       └── gold_fraud_signals.py # Velocity, z-score, geo signals → EventBridge
└── databricks/                 # Databricks Free Edition notebooks
    ├── 00_setup.py             # Shared paths (auto-detects Unity Catalog)
    ├── 01_data_generator.py    # Replaces Kafka on CE — generates synthetic data
    ├── 02_silver.py            # Same logic as streaming/silver, availableNow trigger
    ├── 03_gold.py              # Same logic as streaming/gold, availableNow trigger
    ├── 04_explore.py           # Portfolio queries: fraud breakdown, z-scores, lineage
    └── DATABRICKS_SETUP.md
```

---

## Measured Results (Databricks Free Edition, 50k events)

| Metric | Value |
|---|---|
| Dataset size | 50,000 transactions |
| Realized fraud rate | 1.96% (979 / 50,000) |
| High-confidence alerts (score ≥ 0.70) | 261 (0.52%) |
| Silver dedup drop rate | 0% (no duplicates in generated data) |
| Amount spike avg transaction | $1,834 vs $47 normal (39× spike) |
| Amount spike max transaction | $2,995.61 |
| Velocity burst events detected | 598 |
| Impossible travel events detected | 129 |
| Z-score > 3 fraud events | 248 (all amount spikes) |

**Fraud by country** (Top 3 by rate): CA 2.59% · GB 2.49% · FR 2.45% · DE 2.43%

**Local Docker producer** (configurable via `.env`): tested at 1k–50k events/min

---

## Roadmap

- [x] Kafka producer — 1k–50k events/min, three fraud patterns, Prometheus metrics
- [x] PySpark Structured Streaming — bronze/silver/gold Delta medallion (local Docker)
- [x] Grafana dashboard — events/min, fraud rate, throughput, processing lag
- [x] AWS alerting — Lambda + EventBridge + SES (code complete; deploy with `sam deploy`)
- [x] Databricks Free Edition — full pipeline on serverless compute, Unity Catalog volumes
