from __future__ import annotations

from datetime import date

from pourstream.events import EventRejected, parse_event
from pourstream.generator import SimConfig, simulate

SMALL = SimConfig(start_date=date(2026, 10, 1), days=2, sessions_per_store_day=40, seed=7)


def test_simulation_is_deterministic():
    a, b = simulate(SMALL), simulate(SMALL)
    assert [m.value for m in a.messages] == [m.value for m in b.messages]
    assert simulate(SimConfig(**{**SMALL.__dict__, "seed": 8})).messages != a.messages


def test_messages_are_in_arrival_order_and_fault_rates_are_close_to_config():
    result = simulate(SMALL)
    arrivals = [m.arrival_ts for m in result.messages]
    assert arrivals == sorted(arrivals)
    total = sum(result.kinds.values())
    assert abs(result.kinds["duplicate"] / total - SMALL.duplicate_rate) < 0.01
    assert abs(result.kinds["late"] / total - SMALL.late_rate) < 0.01
    assert 0 < result.kinds["malformed"] / total < 0.015


def test_exactly_the_malformed_messages_break_the_contract():
    for msg in simulate(SMALL).messages:
        if msg.kind == "malformed":
            try:
                parse_event(msg.value, arrived_at=msg.arrival_ts)
            except EventRejected:
                continue
            raise AssertionError(f"malformed message passed the contract: {msg.value!r}")
        parse_event(msg.value, arrived_at=msg.arrival_ts)


def test_late_messages_arrive_long_after_their_event_time():
    for msg in simulate(SMALL).messages:
        if msg.kind == "late":
            event = parse_event(msg.value, arrived_at=msg.arrival_ts)
            assert (msg.arrival_ts - event.event_ts).total_seconds() >= 20 * 60
