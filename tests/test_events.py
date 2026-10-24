from __future__ import annotations

import json
from datetime import UTC

import pytest

from pourstream.events import EventRejected, parse_event
from tests.conftest import NOW, make_event


def _raw(**overrides) -> bytes:
    return json.dumps(make_event(**overrides)).encode()


def test_valid_event_parses_and_normalises_to_utc():
    event = parse_event(_raw(event_ts="2026-10-01T08:30:00+08:00"), arrived_at=NOW)
    assert event.event_ts.tzinfo == UTC
    assert event.event_ts.hour == 0
    assert str(event.amount) == "125.00"


@pytest.mark.parametrize(
    ("raw", "error_type"),
    [
        (b'{"event_id": "abc', "invalid_json"),
        (b"[1, 2, 3]", "invalid_json"),
        (b"\xff\xfe", "invalid_json"),
        (_raw(order_id=None), "schema_violation"),  # order events need order_id
        (_raw(amount=None), "schema_violation"),  # order_placed needs amount
        (_raw(store_id="store-001"), "schema_violation"),
        (_raw(event_ts="2026-10-01T00:30:00"), "schema_violation"),  # naive timestamp
        (_raw(event_type="refund_issued"), "schema_violation"),
        (_raw(unexpected="x"), "schema_violation"),  # contract forbids unknown fields
        (_raw(amount="-5"), "schema_violation"),
        (_raw(event_ts="2026-10-01T03:00:00Z"), "future_timestamp"),
    ],
)
def test_contract_violations_are_classified(raw, error_type):
    with pytest.raises(EventRejected) as exc:
        parse_event(raw, arrived_at=NOW)
    assert exc.value.error_type == error_type


def test_item_added_requires_sku_and_quantity():
    raw = _raw(event_type="item_added", order_id=None, amount=None, item_sku="ESP-LAT")
    with pytest.raises(EventRejected, match="quantity"):
        parse_event(raw, arrived_at=NOW)


def test_small_clock_skew_is_tolerated():
    parse_event(_raw(event_ts="2026-10-01T01:04:00Z"), arrived_at=NOW)
