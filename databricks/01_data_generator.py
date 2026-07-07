# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Synthetic Data Generator
# MAGIC
# MAGIC **Why this notebook exists:**
# MAGIC Databricks Community Edition clusters cannot reach your local Kafka broker
# MAGIC (it runs on `localhost` on your laptop, behind NAT). This notebook replaces
# MAGIC the Kafka producer by generating synthetic transactions directly in Python
# MAGIC and writing them to the bronze Delta table on DBFS.
# MAGIC
# MAGIC **For production Databricks (not CE):** delete this notebook and run
# MAGIC `streaming/jobs/bronze_ingestion.py` pointed at a cloud Kafka cluster
# MAGIC (Confluent Cloud free tier, Amazon MSK, or Azure Event Hubs with Kafka API).
# MAGIC The silver and gold notebooks are identical in both cases.
# MAGIC
# MAGIC **Run order:** `00_setup` → **`01_data_generator`** → `02_silver` → `03_gold` → `04_explore`

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transaction & Fraud Logic
# MAGIC
# MAGIC Self-contained copy of the core producer logic (minus the Kafka send).
# MAGIC The three fraud patterns are identical to the local producer:
# MAGIC - **amount_spike**: $500–$3000 on a card that normally spends $10–$80
# MAGIC - **velocity_burst**: 8–15 transactions from the same card in 60 seconds
# MAGIC - **impossible_travel**: same card used >2,000 km apart within 5 minutes

# COMMAND ----------

import math
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

# ── City coordinates for realistic geo distribution ────────────────────────
CITY_COORDS = {
    "US": [(40.71, -74.01), (34.05, -118.24), (41.88, -87.63)],
    "GB": [(51.51, -0.13), (53.48, -2.24)],
    "CA": [(43.65, -79.38), (45.50, -73.57)],
    "DE": [(52.52, 13.40), (48.14, 11.58)],
    "FR": [(48.85, 2.35), (43.30, 5.37)],
    "AU": [(-33.87, 151.21)],
    "JP": [(35.68, 139.69)],
}
HOME_COUNTRY_WEIGHTS = {"US": 0.60, "GB": 0.10, "CA": 0.10, "DE": 0.08, "FR": 0.07, "AU": 0.03, "JP": 0.02}
CATEGORIES = ["grocery", "gas", "restaurant", "retail", "travel", "online"]
CAT_WEIGHTS = [0.22, 0.15, 0.23, 0.20, 0.08, 0.12]
FAR_CITIES = [("GB", 51.51, -0.13), ("JP", 35.68, 139.69), ("AU", -33.87, 151.21), ("BR", -23.55, -46.63)]


def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _pick_country():
    return random.choices(list(HOME_COUNTRY_WEIGHTS), weights=list(HOME_COUNTRY_WEIGHTS.values()))[0]


def _city(country):
    cities = CITY_COORDS.get(country, CITY_COORDS["US"])
    lat, lon = random.choice(cities)
    return lat + random.uniform(-0.2, 0.2), lon + random.uniform(-0.2, 0.2)


@dataclass
class CardProfile:
    card_id: str
    home_country: str
    home_lat: float
    home_lon: float
    typical_spend_low: float
    typical_spend_high: float
    last_time: Optional[datetime] = None
    last_country: Optional[str] = None
    last_lat: Optional[float] = None
    last_lon: Optional[float] = None
    velocity_window: list = field(default_factory=list)
    burst_remaining: int = 0


@dataclass
class Merchant:
    merchant_id: str
    merchant_category: str
    country: str
    lat: float
    lon: float


class TransactionGenerator:
    def __init__(self, num_cards=500, num_merchants=200):
        self.cards = {}
        self.merchants = []
        for i in range(num_merchants):
            cat = random.choices(CATEGORIES, weights=CAT_WEIGHTS)[0]
            if cat == "online":
                self.merchants.append(Merchant(f"MERCH-{i:04d}", cat, "US", 0.0, 0.0))
            else:
                country = _pick_country()
                lat, lon = _city(country)
                self.merchants.append(Merchant(f"MERCH-{i:04d}", cat, country,
                                               lat + random.uniform(-0.05, 0.05),
                                               lon + random.uniform(-0.05, 0.05)))
        for i in range(num_cards):
            country = _pick_country()
            lat, lon = _city(country)
            low = max(5.0, random.gauss(15, 8))
            self.cards[f"CARD-{i:04d}"] = CardProfile(
                card_id=f"CARD-{i:04d}", home_country=country,
                home_lat=lat, home_lon=lon,
                typical_spend_low=round(low, 2),
                typical_spend_high=round(low + random.uniform(30, 100), 2),
            )

    def pick_card_id(self):
        return random.choice(list(self.cards.keys()))

    def generate(self, card_id: str, fraud_decision) -> dict:
        card = self.cards[card_id]
        now = datetime.now(tz=timezone.utc)
        cutoff = now - timedelta(seconds=60)
        card.velocity_window = [t for t in card.velocity_window if t > cutoff]

        if fraud_decision["pattern"]:
            txn = fraud_decision["pattern"](card, self.merchants, now)
        else:
            near = [m for m in self.merchants if m.country == card.home_country] or self.merchants
            m = random.choice(near)
            txn = {"amount": round(random.uniform(card.typical_spend_low, card.typical_spend_high), 2),
                   "merchant_id": m.merchant_id, "merchant_category": m.merchant_category,
                   "lat": m.lat, "lon": m.lon, "country": m.country,
                   "is_fraud": False, "fraud_type": None}

        card.last_time, card.last_country = now, txn["country"]
        card.last_lat, card.last_lon = txn["lat"], txn["lon"]
        card.velocity_window.append(now)
        txn.update({"transaction_id": str(uuid.uuid4()), "card_id": card_id,
                    "timestamp": now.isoformat().replace("+00:00", "Z")})
        return txn


def _apply_spike(card, merchants, now):
    near = [m for m in merchants if m.country == card.home_country] or merchants
    m = random.choice(near)
    return {"amount": round(random.uniform(500.0, 3000.0), 2), "merchant_id": m.merchant_id,
            "merchant_category": m.merchant_category, "lat": m.lat, "lon": m.lon,
            "country": m.country, "is_fraud": True, "fraud_type": "amount_spike"}


def _apply_burst(card, merchants, now):
    if card.burst_remaining > 0:
        card.burst_remaining -= 1
    near = [m for m in merchants if m.country == card.home_country] or merchants
    m = random.choice(near)
    return {"amount": round(random.uniform(card.typical_spend_low, card.typical_spend_high), 2),
            "merchant_id": m.merchant_id, "merchant_category": m.merchant_category,
            "lat": m.lat, "lon": m.lon, "country": m.country,
            "is_fraud": True, "fraud_type": "velocity_burst"}


def _apply_travel(card, merchants, now):
    last_lat = card.last_lat or card.home_lat
    last_lon = card.last_lon or card.home_lon
    far = [m for m in merchants if m.lat != 0.0 and
           m.country != (card.last_country or card.home_country) and
           _haversine_km(last_lat, last_lon, m.lat, m.lon) >= 2000]
    if far:
        m = random.choice(far)
        lat, lon, country, mid, mcat = m.lat, m.lon, m.country, m.merchant_id, m.merchant_category
    else:
        country, lat, lon = random.choice(FAR_CITIES)
        m = random.choice(merchants)
        mid, mcat = m.merchant_id, m.merchant_category
    return {"amount": round(random.uniform(card.typical_spend_low, card.typical_spend_high), 2),
            "merchant_id": mid, "merchant_category": mcat,
            "lat": round(lat, 4), "lon": round(lon, 4), "country": country,
            "is_fraud": True, "fraud_type": "impossible_travel"}


class FraudInjector:
    def __init__(self, fraud_rate=0.015):
        self._target = fraud_rate
        self._fraud_count = 0
        self._total = 0

    def decide(self, card):
        self._total += 1
        if card.burst_remaining > 0:
            card.burst_remaining -= 1
            self._fraud_count += 1
            return {"pattern": _apply_burst}
        current = self._fraud_count / self._total if self._total else 0.0
        adj = min(max(self._target * (1 + (self._target - current) / max(self._target, 0.001)), 0), 1)
        if random.random() >= adj:
            return {"pattern": None}
        # pick pattern
        if card.last_time and (datetime.now(tz=timezone.utc) - card.last_time).seconds < 300 and random.random() < 0.3:
            self._fraud_count += 1
            return {"pattern": _apply_travel}
        if random.random() < 0.3:
            card.burst_remaining = random.randint(7, 14)
            self._fraud_count += 1
            return {"pattern": _apply_burst}
        self._fraud_count += 1
        return {"pattern": _apply_spike}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate & Write to Bronze Delta
# MAGIC
# MAGIC Adjust `NUM_EVENTS` to control dataset size.
# MAGIC 50k events takes ~30 seconds on a CE cluster.

# COMMAND ----------

from pyspark.sql.types import (
    BooleanType, DoubleType, StringType, StructField, StructType
)

SCHEMA = StructType([
    StructField("transaction_id",    StringType(),  True),
    StructField("card_id",           StringType(),  True),
    StructField("amount",            DoubleType(),  True),
    StructField("merchant_id",       StringType(),  True),
    StructField("merchant_category", StringType(),  True),
    StructField("lat",               DoubleType(),  True),
    StructField("lon",               DoubleType(),  True),
    StructField("timestamp",         StringType(),  True),
    StructField("country",           StringType(),  True),
    StructField("is_fraud",          BooleanType(), True),
    StructField("fraud_type",        StringType(),  True),
])

# COMMAND ----------

NUM_EVENTS = 50_000    # ~30 seconds on CE; increase for a larger dataset
FRAUD_RATE  = 0.015

gen      = TransactionGenerator(num_cards=500, num_merchants=200)
injector = FraudInjector(fraud_rate=FRAUD_RATE)

rows = []
for _ in range(NUM_EVENTS):
    card_id  = gen.pick_card_id()
    decision = injector.decide(gen.cards[card_id])
    rows.append(gen.generate(card_id, decision))

df = spark.createDataFrame(rows, schema=SCHEMA)

(df.write
   .format("delta")
   .mode("append")
   .save(BRONZE_PATH))

fraud_count = sum(1 for r in rows if r["is_fraud"])
print(f"Written {NUM_EVENTS:,} rows to {BRONZE_PATH}")
print(f"Fraud events: {fraud_count:,} ({100*fraud_count/NUM_EVENTS:.2f}%)")

# COMMAND ----------

display(
    spark.read.format("delta").load(BRONZE_PATH)
    .filter("is_fraud = true")
    .select("card_id", "fraud_type", "amount", "country", "timestamp")
    .orderBy("timestamp", ascending=False)
    .limit(20)
)
