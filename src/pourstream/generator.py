"""Deterministic simulator of mobile/web/kiosk ordering sessions.

It produces messages in *arrival* order, the way a broker would receive them, and injects
the faults a real event stream has: client retries (duplicates), devices that buffer
events offline (late arrivals) and broken clients (malformed payloads).
"""

from __future__ import annotations

import csv
import json
import random
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import accumulate
from pathlib import Path

from pourstream.config import SEEDS_DIR
from pourstream.events import OrderEvent

MANILA_OFFSET = timedelta(hours=8)  # Asia/Manila has no DST.

# Relative session volume by local hour (stores open 06:00-22:00).
HOUR_WEIGHTS = {
    6: 4, 7: 9, 8: 10, 9: 7, 10: 5, 11: 6, 12: 8, 13: 7,
    14: 5, 15: 6, 16: 6, 17: 7, 18: 6, 19: 5, 20: 3, 21: 2,
}  # fmt: skip
PEAK_HOURS = {7, 8, 12, 13, 17}
FORMAT_VOLUME = {"flagship": 1.6, "standard": 1.0, "kiosk": 0.6}
CHANNEL_WEIGHTS = {"app": 0.72, "web": 0.10, "kiosk": 0.18}
PAYMENT_WEIGHTS = {
    "app": {"gcash": 5, "maya": 2, "card": 3},
    "web": {"card": 6, "gcash": 3, "maya": 1},
}


@dataclass(frozen=True)
class SimConfig:
    start_date: date = date(2026, 10, 1)
    days: int = 7
    sessions_per_store_day: int = 90
    customers: int = 6000
    duplicate_rate: float = 0.02
    late_rate: float = 0.03
    malformed_rate: float = 0.005
    seed: int = 42


@dataclass(frozen=True, order=True)
class SimMessage:
    arrival_ts: datetime
    key: str = field(compare=False)
    value: bytes = field(compare=False)
    kind: str = field(compare=False)  # clean | late | duplicate | malformed


@dataclass
class SimResult:
    messages: list[SimMessage]
    unique_events: int
    orders: int
    kinds: Counter[str]


def _load_seed(name: str, seeds_dir: Path) -> list[dict[str, str]]:
    with (seeds_dir / name).open(newline="") as fh:
        return list(csv.DictReader(fh))


def _weighted(rng: random.Random, weights: dict[str, float]) -> str:
    return rng.choices(list(weights), weights=list(weights.values()))[0]


class _SessionBuilder:
    def __init__(self, rng: random.Random, store: dict[str, str], menu: list[dict[str, str]]):
        self.rng = rng
        self.store = store
        self.menu = menu
        self.events: list[OrderEvent] = []

    def _uuid(self) -> uuid.UUID:
        return uuid.UUID(int=self.rng.getrandbits(128), version=4)

    def emit(self, ts: datetime, event_type: str, **fields) -> None:
        self.events.append(
            OrderEvent(
                event_id=self._uuid(),
                event_type=event_type,
                event_ts=ts,
                store_id=self.store["store_id"],
                **fields,
            )
        )

    def build(self, start: datetime, customer_id: str | None, channel: str) -> int:
        """Simulate one session; return 1 if it placed an order."""
        rng = self.rng
        base = {
            "session_id": f"S-{self._uuid().hex[:12]}",
            "customer_id": customer_id,
            "channel": channel,
        }
        ts = start
        self.emit(ts, "session_started", **base)
        if rng.random() > 0.92:
            return 0
        ts += timedelta(seconds=rng.randint(3, 40))
        self.emit(ts, "menu_viewed", **base)
        if rng.random() > 0.50:
            return 0

        cart = Decimal("0")
        for _ in range(rng.choices([1, 2, 3], weights=[6, 3, 1])[0]):
            item = rng.choice(self.menu)
            qty = rng.choices([1, 2, 3], weights=[8, 2, 0.5])[0]
            cart += Decimal(item["unit_price"]) * qty
            ts += timedelta(seconds=rng.randint(5, 60))
            self.emit(ts, "item_added", item_sku=item["sku"], quantity=qty, **base)
        if rng.random() > 0.75:
            return 0
        ts += timedelta(seconds=rng.randint(5, 45))
        self.emit(ts, "checkout_started", **base)
        if rng.random() > 0.92:
            return 0

        order = {**base, "order_id": f"O-{self._uuid().hex[:12]}"}
        ts += timedelta(seconds=rng.randint(10, 60))
        self.emit(ts, "order_placed", amount=cart, **order)

        method = "cash" if channel == "kiosk" else _weighted(rng, PAYMENT_WEIGHTS[channel])
        if rng.random() < 0.05:
            ts += timedelta(seconds=rng.randint(2, 15))
            self.emit(ts, "payment_failed", payment_method=method, **order)
            if rng.random() < 0.5:
                ts += timedelta(seconds=rng.randint(30, 120))
                self.emit(ts, "order_cancelled", cancel_reason="payment_failed", **order)
                return 1
        ts += timedelta(seconds=rng.randint(2, 20))
        self.emit(ts, "payment_succeeded", payment_method=method, **order)

        if rng.random() < 0.015:
            ts += timedelta(seconds=rng.randint(30, 180))
            self.emit(ts, "order_cancelled", cancel_reason="customer_request", **order)
            return 1
        local_hour = (start + MANILA_OFFSET).hour
        prep_mean = 420 if local_hour in PEAK_HOURS else 270
        prep_mean *= 1.2 if self.store["format"] == "kiosk" else 1.0
        ts += timedelta(seconds=max(90, int(rng.gauss(prep_mean, prep_mean * 0.3))))
        self.emit(ts, "order_ready", **order)
        if rng.random() < 0.02:
            ts += timedelta(minutes=60)
            self.emit(ts, "order_cancelled", cancel_reason="not_picked_up", **order)
            return 1
        ts += timedelta(seconds=rng.randint(30, 900))
        self.emit(ts, "order_picked_up", **order)
        return 1


def _corrupt(rng: random.Random, event: OrderEvent) -> bytes:
    payload = json.loads(event.to_json())
    defect = rng.choice(
        ["truncated", "missing_field", "bad_store", "negative_qty", "naive_ts", "future_ts"]
    )
    if defect == "truncated":
        return event.to_json()[: rng.randint(10, 40)].encode()
    if defect == "missing_field":
        payload.pop("session_id", None)
    elif defect == "bad_store":
        payload["store_id"] = "store-" + payload["store_id"][-3:]
    elif defect == "negative_qty":
        payload.update(event_type="item_added", item_sku="ESP-LAT", quantity=-1)
    elif defect == "naive_ts":
        payload["event_ts"] = payload["event_ts"].replace("Z", "")
    else:  # device clock ahead by more than the max late lag (6h)
        payload["event_ts"] = (event.event_ts + timedelta(hours=rng.randint(8, 14))).isoformat()
    return json.dumps(payload).encode()


def simulate(config: SimConfig | None = None, seeds_dir: Path = SEEDS_DIR) -> SimResult:
    config = config or SimConfig()
    rng = random.Random(config.seed)
    stores = _load_seed("stores.csv", seeds_dir)
    menu = _load_seed("menu_items.csv", seeds_dir)
    hours, hour_w = list(HOUR_WEIGHTS), list(HOUR_WEIGHTS.values())
    # Zipf-like loyalty: a minority of regulars place a large share of orders.
    customer_ids = [f"C{i:05d}" for i in range(1, config.customers + 1)]
    customer_cum = list(accumulate(1 / (i**0.5) for i in range(1, config.customers + 1)))

    events: list[OrderEvent] = []
    orders = 0
    for day_idx in range(config.days):
        day = config.start_date + timedelta(days=day_idx)
        weekend = 0.85 if day.weekday() >= 5 else 1.0
        for store in stores:
            builder = _SessionBuilder(rng, store, menu)
            n_sessions = int(
                config.sessions_per_store_day * FORMAT_VOLUME[store["format"]] * weekend
                * rng.uniform(0.85, 1.15)
            )  # fmt: skip
            for _ in range(n_sessions):
                hour = rng.choices(hours, weights=hour_w)[0]
                local = datetime(day.year, day.month, day.day, hour, tzinfo=UTC) + timedelta(
                    seconds=rng.randint(0, 3599)
                )
                start = local - MANILA_OFFSET
                channel = _weighted(rng, CHANNEL_WEIGHTS)
                anonymous = channel == "kiosk" and rng.random() < 0.7
                customer = (
                    None if anonymous else rng.choices(customer_ids, cum_weights=customer_cum)[0]
                )
                orders += builder.build(start, customer, channel)
            events.extend(builder.events)

    messages: list[SimMessage] = []
    kinds: Counter[str] = Counter()
    for event in events:
        raw = event.to_json().encode()
        lag = timedelta(milliseconds=rng.randint(50, 2500))
        kind = "clean"
        if rng.random() < config.late_rate:
            lag = timedelta(minutes=rng.randint(20, 360))  # buffered offline, flushed later
            kind = "late"
        if rng.random() < config.malformed_rate:
            raw, kind = _corrupt(rng, event), "malformed"
        arrival = event.event_ts + lag
        messages.append(SimMessage(arrival, event.session_id, raw, kind))
        kinds[kind] += 1
        if kind != "malformed" and rng.random() < config.duplicate_rate:
            retry = arrival + timedelta(seconds=rng.randint(1, 30))
            messages.append(SimMessage(retry, event.session_id, raw, "duplicate"))
            kinds["duplicate"] += 1

    messages.sort()
    return SimResult(
        messages=messages,
        unique_events=len(events) - kinds["malformed"],
        orders=orders,
        kinds=kinds,
    )
