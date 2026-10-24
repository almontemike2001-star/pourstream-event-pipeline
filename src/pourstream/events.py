"""The order-event data contract.

Every message on ``app.order_events`` must parse into :class:`OrderEvent`. Messages that
do not are routed to the dead-letter queue with a machine-readable ``error_type``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

EventType = Literal[
    "session_started",
    "menu_viewed",
    "item_added",
    "checkout_started",
    "order_placed",
    "payment_failed",
    "payment_succeeded",
    "order_ready",
    "order_picked_up",
    "order_cancelled",
]
Channel = Literal["app", "web", "kiosk"]
PaymentMethod = Literal["gcash", "maya", "card", "cash"]

ORDER_EVENTS = frozenset(
    {
        "order_placed",
        "payment_failed",
        "payment_succeeded",
        "order_ready",
        "order_picked_up",
        "order_cancelled",
    }
)

# Events stamped further than this into the future (vs. broker arrival time) are rejected:
# they come from devices with broken clocks and would corrupt time-based aggregates.
MAX_FUTURE_SKEW = timedelta(minutes=5)


class OrderEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    event_id: UUID
    event_type: EventType
    event_ts: datetime
    session_id: str = Field(min_length=1)
    customer_id: str | None = None
    store_id: str = Field(pattern=r"^ST\d{3}$")
    channel: Channel
    order_id: str | None = None
    item_sku: str | None = None
    quantity: int | None = Field(default=None, ge=1, le=20)
    amount: Decimal | None = Field(default=None, ge=0, max_digits=10, decimal_places=2)
    payment_method: PaymentMethod | None = None
    cancel_reason: str | None = None

    @field_validator("event_ts")
    @classmethod
    def _tz_aware_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("event_ts must include a timezone offset")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _required_fields_per_type(self) -> OrderEvent:
        missing: list[str] = []
        if self.event_type in ORDER_EVENTS and not self.order_id:
            missing.append("order_id")
        if self.event_type == "item_added":
            missing += [f for f in ("item_sku", "quantity") if getattr(self, f) is None]
        if self.event_type == "order_placed" and self.amount is None:
            missing.append("amount")
        if self.event_type in {"payment_failed", "payment_succeeded"} and not self.payment_method:
            missing.append("payment_method")
        if self.event_type == "order_cancelled" and not self.cancel_reason:
            missing.append("cancel_reason")
        if missing:
            raise ValueError(f"{self.event_type} requires: {', '.join(missing)}")
        return self

    def to_json(self) -> str:
        return self.model_dump_json(exclude_none=True)


class EventRejected(Exception):
    """Raised when a raw message violates the contract."""

    def __init__(self, error_type: str, detail: str):
        super().__init__(f"{error_type}: {detail}")
        self.error_type = error_type
        self.detail = detail


def parse_event(raw: bytes, arrived_at: datetime) -> OrderEvent:
    """Parse and validate one raw message.

    ``error_type`` is one of ``invalid_json``, ``schema_violation`` or ``future_timestamp``.
    """
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EventRejected("invalid_json", str(exc)) from exc
    if not isinstance(payload, dict):
        raise EventRejected("invalid_json", "payload is not a JSON object")
    try:
        event = OrderEvent.model_validate(payload)
    except ValidationError as exc:
        detail = "; ".join(
            f"{'.'.join(map(str, e['loc'])) or '<root>'}: {e['msg']}" for e in exc.errors()
        )
        raise EventRejected("schema_violation", detail) from exc
    if event.event_ts - arrived_at > MAX_FUTURE_SKEW:
        raise EventRejected(
            "future_timestamp", f"event_ts {event.event_ts.isoformat()} is ahead of arrival"
        )
    return event
