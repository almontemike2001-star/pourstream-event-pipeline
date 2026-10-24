"""Round trip through a real broker (Redpanda/Kafka). Skipped when none is reachable."""

from __future__ import annotations

import uuid
from dataclasses import replace
from datetime import date

import duckdb
import pytest

from pourstream.generator import SimConfig
from pourstream.loader import Lake, run_loader
from pourstream.pipeline import produce
from pourstream.transport import KafkaConsumer

pytestmark = pytest.mark.kafka

SIM = SimConfig(start_date=date(2026, 10, 1), days=1, sessions_per_store_day=15, seed=5)


def test_kafka_round_trip_commits_offsets(settings):
    settings = replace(settings, topic=f"test.order_events.{uuid.uuid4().hex[:8]}")
    sim = produce(settings, "kafka", SIM)
    lake = Lake(settings.lake_dir)

    def consume():
        consumer = KafkaConsumer(settings.kafka_bootstrap, settings.topic, settings.consumer_group)
        try:
            return run_loader(
                consumer, lake, batch_size=500, poll_timeout_s=3, idle_polls_before_exit=2
            )
        finally:
            consumer.close()

    first = consume()
    assert first.messages == len(sim.messages)
    assert first.dead_lettered == sim.kinds["malformed"]

    assert consume().messages == 0  # offsets were committed: nothing is re-read

    unique = (
        duckdb.connect()
        .execute(
            "select count(distinct event_id) from read_parquet(?)",
            [f"{settings.lake_dir}/bronze/order_events/*/*.parquet"],
        )
        .fetchone()[0]
    )
    assert unique == sim.unique_events
