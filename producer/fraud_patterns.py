"""
Fraud pattern injection for FraudFlow.

Design: each pattern is a self-contained class with two methods:
  - should_activate(card, now) -> bool   — can this pattern fire on this card right now?
  - apply(card, merchant_pool, now) -> dict — build the (partial) transaction dict

FraudInjector orchestrates them and enforces the global fraud rate.
"""

import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional


# ---------------------------------------------------------------------------
# Haversine distance (no external deps — ~8 lines of math beats importing geopy)
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# Known far-apart city pairs for impossible-travel fallback when the merchant pool
# doesn't have a qualifying distant merchant.
_FAR_CITIES: list[tuple[str, float, float]] = [
    ("GB", 51.51, -0.13),
    ("JP", 35.68, 139.69),
    ("AU", -33.87, 151.21),
    ("BR", -23.55, -46.63),
    ("SG", 1.35, 103.82),
    ("ZA", -26.20, 28.04),
]


# ---------------------------------------------------------------------------
# Pattern 1: Amount Spike
# ---------------------------------------------------------------------------

class AmountSpikePattern:
    """
    A card that normally spends $10–$80 suddenly submits a $500–$3000 transaction.
    Stateless — no per-card memory is needed because each spike is a one-off event.
    The fraud is in the amount; location and merchant remain plausible.
    """

    def should_activate(self, card, now: datetime) -> bool:
        # Activate with a small probability per call. FraudInjector controls the
        # overall rate; this controls what share of fraud events are spikes.
        return random.random() < 0.4

    def apply(self, card, merchant_pool: list, now: datetime) -> dict:
        from transaction_generator import MerchantRecord
        near = [m for m in merchant_pool if m.country == card.home_country]
        merchant = random.choice(near) if near else random.choice(merchant_pool)
        return {
            "amount": round(random.uniform(500.0, 3000.0), 2),
            "merchant_id": merchant.merchant_id,
            "merchant_category": merchant.merchant_category,
            "lat": merchant.lat,
            "lon": merchant.lon,
            "country": merchant.country,
            "is_fraud": True,
            "fraud_type": "amount_spike",
        }


# ---------------------------------------------------------------------------
# Pattern 2: Velocity Burst
# ---------------------------------------------------------------------------

class VelocityBurstPattern:
    """
    The same card fires 8–15 transactions within a 60-second window.
    Normal cards average 1–3 transactions per hour.
    The fraud is in the *frequency*, not the transaction content — each individual
    transaction looks legitimate. This mimics a compromised card being used by a
    bot before the issuer can freeze it.
    """

    def should_activate(self, card, now: datetime) -> bool:
        # If we're already mid-burst, keep activating.
        if card.velocity_burst_remaining > 0:
            return True
        # Start a new burst with a small probability.
        if random.random() < 0.3:
            card.velocity_burst_remaining = random.randint(8, 15)
            return True
        return False

    def apply(self, card, merchant_pool: list, now: datetime) -> dict:
        from transaction_generator import MerchantRecord
        if card.velocity_burst_remaining > 0:
            card.velocity_burst_remaining -= 1

        near = [m for m in merchant_pool if m.country == card.home_country]
        merchant = random.choice(near) if near else random.choice(merchant_pool)
        # Amount looks normal — velocity is the signal, not the amount.
        amount = round(random.uniform(card.typical_spend_low, card.typical_spend_high), 2)
        return {
            "amount": amount,
            "merchant_id": merchant.merchant_id,
            "merchant_category": merchant.merchant_category,
            "lat": merchant.lat,
            "lon": merchant.lon,
            "country": merchant.country,
            "is_fraud": True,
            "fraud_type": "velocity_burst",
        }


# ---------------------------------------------------------------------------
# Pattern 3: Impossible Travel
# ---------------------------------------------------------------------------

class ImpossibleTravelPattern:
    """
    The same card is used in two countries more than 2,000 km apart within 5 minutes.
    A human cannot travel that distance that quickly, so the second transaction is
    either a card clone or an online fraud using stolen credentials.
    """

    _MIN_DISTANCE_KM = 2000.0
    _MAX_GAP_MINUTES = 5

    def should_activate(self, card, now: datetime) -> bool:
        # Only meaningful if the card has a recent transaction to compare against.
        if card.last_transaction_time is None:
            return False
        age = (now - card.last_transaction_time).total_seconds() / 60.0
        if age > self._MAX_GAP_MINUTES:
            return False
        return random.random() < 0.3

    def apply(self, card, merchant_pool: list, now: datetime) -> dict:
        last_lat = card.last_transaction_lat or card.home_lat
        last_lon = card.last_transaction_lon or card.home_lon

        # Find a merchant that is physically impossible to reach in 5 minutes.
        far_merchants = [
            m
            for m in merchant_pool
            if m.country != (card.last_transaction_country or card.home_country)
            and m.lat != 0.0  # exclude online merchants
            and _haversine_km(last_lat, last_lon, m.lat, m.lon) >= self._MIN_DISTANCE_KM
        ]

        if far_merchants:
            merchant = random.choice(far_merchants)
            lat, lon, country = merchant.lat, merchant.lon, merchant.country
            merchant_id = merchant.merchant_id
            merchant_category = merchant.merchant_category
        else:
            # Fallback: pick a synthetic far-away city.
            city = random.choice(_FAR_CITIES)
            country, lat, lon = city
            while _haversine_km(last_lat, last_lon, lat, lon) < self._MIN_DISTANCE_KM:
                city = random.choice(_FAR_CITIES)
                country, lat, lon = city
            # Use the closest merchant id/category for plausibility.
            m = random.choice(merchant_pool)
            merchant_id, merchant_category = m.merchant_id, m.merchant_category

        amount = round(random.uniform(card.typical_spend_low, card.typical_spend_high), 2)
        return {
            "amount": amount,
            "merchant_id": merchant_id,
            "merchant_category": merchant_category,
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "country": country,
            "is_fraud": True,
            "fraud_type": "impossible_travel",
        }


# ---------------------------------------------------------------------------
# FraudInjector — orchestrates patterns and enforces the global fraud rate
# ---------------------------------------------------------------------------

@dataclass
class FraudDecision:
    pattern: Optional[object]  # None = legitimate transaction


class FraudInjector:
    """
    Controls *whether* fraud fires (rate targeting) and *which* pattern applies.

    Rate control: tracks a running fraud_count/total_count ratio and dynamically
    adjusts the probability of injecting fraud each event so the realized rate
    converges to the configured FRAUD_RATE over time.
    """

    def __init__(self, fraud_rate: float) -> None:
        self._target_rate = fraud_rate
        self._fraud_count = 0
        self._total_count = 0
        self._patterns = [
            AmountSpikePattern(),
            VelocityBurstPattern(),
            ImpossibleTravelPattern(),
        ]

    def decide(self, card) -> FraudDecision:
        self._total_count += 1
        now = datetime.now(tz=timezone.utc)

        # Dynamic probability: if we're running behind the target rate, boost the
        # injection chance; if we're above it, suppress it toward zero.
        # A mid-burst card bypasses rate control so the burst doesn't get cut short.
        if card.velocity_burst_remaining > 0:
            should_fraud = True
        else:
            current_rate = self._fraud_count / self._total_count if self._total_count else 0.0
            # Simple proportional controller: scale up/down relative to the gap.
            adjustment = 1.0 + (self._target_rate - current_rate) / max(self._target_rate, 0.001)
            adjusted_prob = min(max(self._target_rate * adjustment, 0.0), 1.0)
            should_fraud = random.random() < adjusted_prob

        if not should_fraud:
            return FraudDecision(pattern=None)

        # Try each pattern in order; use the first that says it can activate.
        for pattern in self._patterns:
            if pattern.should_activate(card, now):
                self._fraud_count += 1
                return FraudDecision(pattern=pattern)

        # No pattern activated — treat as legitimate to avoid inflating the fraud count.
        return FraudDecision(pattern=None)
