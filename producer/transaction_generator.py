import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from config import Config

CITY_COORDS: dict[str, list[tuple[float, float]]] = {
    "US": [(40.71, -74.01), (34.05, -118.24), (41.88, -87.63), (29.76, -95.37)],
    "GB": [(51.51, -0.13), (53.48, -2.24)],
    "CA": [(43.65, -79.38), (45.50, -73.57)],
    "DE": [(52.52, 13.40), (48.14, 11.58)],
    "FR": [(48.85, 2.35), (43.30, 5.37)],
    "AU": [(-33.87, 151.21), (-37.81, 144.96)],
    "SG": [(1.35, 103.82)],
    "JP": [(35.68, 139.69)],
    "BR": [(-23.55, -46.63)],
    "MX": [(19.43, -99.13)],
}

HOME_COUNTRY_WEIGHTS = {
    "US": 0.60,
    "GB": 0.10,
    "CA": 0.10,
    "DE": 0.08,
    "FR": 0.07,
    "AU": 0.02,
    "SG": 0.01,
    "JP": 0.01,
    "BR": 0.005,
    "MX": 0.005,
}

MERCHANT_CATEGORIES = ["grocery", "gas", "restaurant", "retail", "travel", "online"]
CATEGORY_WEIGHTS = [0.22, 0.15, 0.23, 0.20, 0.08, 0.12]


@dataclass
class CardProfile:
    card_id: str
    home_country: str
    home_lat: float
    home_lon: float
    typical_spend_low: float
    typical_spend_high: float
    last_transaction_time: Optional[datetime] = None
    last_transaction_country: Optional[str] = None
    last_transaction_lat: Optional[float] = None
    last_transaction_lon: Optional[float] = None
    velocity_window_transactions: list = field(default_factory=list)
    velocity_burst_remaining: int = 0


@dataclass
class MerchantRecord:
    merchant_id: str
    merchant_category: str
    country: str
    lat: float
    lon: float


def _jitter(val: float, amount: float = 0.05) -> float:
    return val + random.uniform(-amount, amount)


def _pick_country() -> str:
    countries = list(HOME_COUNTRY_WEIGHTS.keys())
    weights = list(HOME_COUNTRY_WEIGHTS.values())
    return random.choices(countries, weights=weights, k=1)[0]


def _city_for_country(country: str) -> tuple[float, float]:
    cities = CITY_COORDS.get(country, CITY_COORDS["US"])
    return random.choice(cities)


class TransactionGenerator:
    def __init__(self, config: Config) -> None:
        self._config = config
        self.card_profiles: dict[str, CardProfile] = {}
        self.merchant_pool: list[MerchantRecord] = []
        self._init_merchants()
        self._init_cards()

    def _init_merchants(self) -> None:
        for i in range(self._config.num_merchants):
            category = random.choices(MERCHANT_CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]

            if category == "online":
                self.merchant_pool.append(
                    MerchantRecord(
                        merchant_id=f"MERCH-{i:04d}",
                        merchant_category=category,
                        country="US",
                        lat=0.0,
                        lon=0.0,
                    )
                )
            else:
                country = _pick_country()
                base_lat, base_lon = _city_for_country(country)
                self.merchant_pool.append(
                    MerchantRecord(
                        merchant_id=f"MERCH-{i:04d}",
                        merchant_category=category,
                        country=country,
                        lat=_jitter(base_lat, 0.05),
                        lon=_jitter(base_lon, 0.05),
                    )
                )

    def _init_cards(self) -> None:
        for i in range(self._config.num_cards):
            country = _pick_country()
            base_lat, base_lon = _city_for_country(country)

            low = max(5.0, random.gauss(15, 8))
            high = low + random.uniform(30, 100)

            self.card_profiles[f"CARD-{i:04d}"] = CardProfile(
                card_id=f"CARD-{i:04d}",
                home_country=country,
                home_lat=_jitter(base_lat, 0.2),
                home_lon=_jitter(base_lon, 0.2),
                typical_spend_low=round(low, 2),
                typical_spend_high=round(high, 2),
            )

    def pick_card_id(self) -> str:
        return random.choice(list(self.card_profiles.keys()))

    def merchants_near_country(self, country: str) -> list[MerchantRecord]:
        """Return merchants in the given country, or all merchants if none match."""
        matches = [m for m in self.merchant_pool if m.country == country]
        return matches if matches else self.merchant_pool

    def generate_transaction(self, card_id: str, fraud_decision) -> dict:
        """
        Build one transaction dict. `fraud_decision` is a FraudDecision namedtuple
        from FraudInjector : it carries the pattern instance (or None for legitimate).
        """
        card = self.card_profiles[card_id]
        now = datetime.now(tz=timezone.utc)

        cutoff = now - timedelta(seconds=60)
        card.velocity_window_transactions = [
            t for t in card.velocity_window_transactions if t > cutoff
        ]

        if fraud_decision.pattern is not None:
            txn = fraud_decision.pattern.apply(card, self.merchant_pool, now)
        else:
            merchant = random.choice(self.merchants_near_country(card.home_country))
            amount = round(
                random.uniform(card.typical_spend_low, card.typical_spend_high), 2
            )
            txn = {
                "amount": amount,
                "merchant_id": merchant.merchant_id,
                "merchant_category": merchant.merchant_category,
                "lat": merchant.lat,
                "lon": merchant.lon,
                "country": merchant.country,
                "is_fraud": False,
                "fraud_type": None,
            }

        card.last_transaction_time = now
        card.last_transaction_country = txn["country"]
        card.last_transaction_lat = txn["lat"]
        card.last_transaction_lon = txn["lon"]
        card.velocity_window_transactions.append(now)

        txn.update(
            {
                "transaction_id": str(uuid.uuid4()),
                "card_id": card_id,
                "timestamp": now.isoformat().replace("+00:00", "Z"),
            }
        )
        return txn
