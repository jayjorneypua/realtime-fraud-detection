"""
online features: the same 15 features as sql/transaction_features.sql, computed per transaction from the
card's recent history in redis instead of a window function over the whole table.

every definition mirrors the sql: timestamps are read as UTC, the 1h and 24h windows include the current
transaction, the 30 day windows cover the prior 30 days only, and "previous" is the card's latest earlier
transaction ordered by (epoch, transaction id), like LAG(...) OVER (ORDER BY transaction_epoch, transaction_id).
"""

import calendar
import math
from dataclasses import dataclass
from datetime import date, datetime

SECONDS_IN_HOUR: int = 3600
SECONDS_IN_DAY: int = 86400
LOOKBACK_SECONDS: int = 30 * SECONDS_IN_DAY
EARTH_RADIUS_KM: float = 6371.0


@dataclass(frozen=True)
class CardTransaction:
    transaction_id: str
    transaction_epoch: int
    amount: float
    merchant_category: str
    merchant_name: str
    merchant_latitude: float
    merchant_longitude: float

    @property
    def order_key(self) -> tuple[int, str]:
        return (self.transaction_epoch, self.transaction_id)


def to_epoch(transaction_time: datetime) -> int:
    # spark reads the timestamps as UTC, so the naive time is treated as UTC here too
    return calendar.timegm(transaction_time.timetuple())


def haversine_km(latitude_from: float, longitude_from: float, latitude_to: float, longitude_to: float) -> float:
    haversine_term: float = (
        math.sin(math.radians(latitude_to - latitude_from) / 2) ** 2
        + math.cos(math.radians(latitude_from)) * math.cos(math.radians(latitude_to))
        * math.sin(math.radians(longitude_to - longitude_from) / 2) ** 2
    )
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(haversine_term))


def is_last_day_of_month(day: date) -> bool:
    return day.day == calendar.monthrange(day.year, day.month)[1]


def months_between(later: datetime, earlier: date) -> float:
    # spark MONTHS_BETWEEN: whole months when the days of month match or both are month ends,
    # otherwise the remainder counts as a fraction of a 31 day month, rounded to 8 decimals
    whole_months: int = (later.year - earlier.year) * 12 + (later.month - earlier.month)
    if later.day == earlier.day or (is_last_day_of_month(later.date()) and is_last_day_of_month(earlier)):
        return float(whole_months)
    seconds_of_day: int = later.hour * SECONDS_IN_HOUR + later.minute * 60 + later.second
    remainder_seconds: int = (later.day - earlier.day) * SECONDS_IN_DAY + seconds_of_day
    return round(whole_months + remainder_seconds / (31 * SECONDS_IN_DAY), 8)


def spark_day_of_week(transaction_time: datetime) -> int:
    # spark DAYOFWEEK: 1 = Sunday ... 7 = Saturday; python weekday(): 0 = Monday ... 6 = Sunday
    return (transaction_time.weekday() + 1) % 7 + 1


def build_features(
    current: CardTransaction,
    transaction_time: datetime,
    customer_birth_date: date,
    customer_latitude: float,
    customer_longitude: float,
    history: list[CardTransaction],
    previous: CardTransaction | None,
) -> dict[str, float | str]:
    epoch: int = current.transaction_epoch

    last_hour: list[CardTransaction] = [t for t in history if epoch - SECONDS_IN_HOUR <= t.transaction_epoch <= epoch] + [current]
    last_day: list[CardTransaction] = [t for t in history if epoch - SECONDS_IN_DAY <= t.transaction_epoch <= epoch] + [current]
    prior_lookback: list[CardTransaction] = [t for t in history if epoch - LOOKBACK_SECONDS <= t.transaction_epoch <= epoch - 1]

    prior_average_amount: float | None = (
        sum(t.amount for t in prior_lookback) / len(prior_lookback) if prior_lookback else None
    )
    hour_of_day: int = transaction_time.hour

    return {
        "merchant_category": current.merchant_category,
        "amount": current.amount,
        "hour_of_day": float(hour_of_day),
        "day_of_week": float(spark_day_of_week(transaction_time)),
        "is_night": 1.0 if hour_of_day >= 22 or hour_of_day < 6 else 0.0,
        "customer_age_years": float(math.floor(months_between(transaction_time, customer_birth_date) / 12)),
        "customer_to_merchant_km": haversine_km(customer_latitude, customer_longitude, current.merchant_latitude, current.merchant_longitude),
        "card_transaction_count_1h": float(len(last_hour)),
        "card_amount_sum_1h": sum(t.amount for t in last_hour),
        "card_transaction_count_24h": float(len(last_day)),
        "card_amount_sum_24h": sum(t.amount for t in last_day),
        # sql: COALESCE(TRY_DIVIDE(amount, average), 1.0), so no history or a zero average gives 1.0
        "amount_to_card_average_ratio": current.amount / prior_average_amount if prior_average_amount else 1.0,
        "seconds_since_previous_card_transaction": float(epoch - previous.transaction_epoch) if previous else float(LOOKBACK_SECONDS),
        "km_from_previous_merchant": (
            haversine_km(previous.merchant_latitude, previous.merchant_longitude, current.merchant_latitude, current.merchant_longitude)
            if previous else 0.0
        ),
        "is_new_category_for_card": 0.0 if any(t.merchant_category == current.merchant_category for t in prior_lookback) else 1.0,
        "is_new_merchant_for_card": 0.0 if any(t.merchant_name == current.merchant_name for t in prior_lookback) else 1.0,
    }