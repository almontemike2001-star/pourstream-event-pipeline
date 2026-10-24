"""End-to-end: simulate -> file broker -> loader -> dbt build, then check the marts."""

from __future__ import annotations

from datetime import date

import duckdb
import pytest

from pourstream import pipeline
from pourstream.generator import SimConfig, simulate
from pourstream.report import render
from pourstream.transport import FileProducer

pytestmark = pytest.mark.e2e

SIM = SimConfig(start_date=date(2026, 10, 1), days=2, sessions_per_store_day=40, seed=11)


def _publish(settings, messages):
    producer = FileProducer(settings.file_broker_dir, settings.topic)
    for m in messages:
        producer.send(m.key, m.value, m.arrival_ts)
    producer.flush()
    producer.close()


def _orders(settings):
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        return con.execute(
            "select * exclude (last_ingested_at) from gold.fct_orders order by order_id"
        ).fetchall()


def test_full_pipeline_builds_and_reconciles(settings, tmp_path):
    sim = pipeline.produce(settings, "file", SIM)
    stats = pipeline.load(settings, "file")
    pipeline.transform(settings)

    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        health = con.execute(
            "select sum(total_messages), sum(unique_events), sum(duplicate_deliveries),"
            " sum(dead_lettered_messages) from gold.agg_pipeline_health"
        ).fetchone()
        orders = con.execute("select count(*) from gold.fct_orders").fetchone()[0]
        # A spill-over day holds only stragglers: every event on it arrived late.
        bad_spillover = con.execute(
            "select count(*) from gold.agg_pipeline_health"
            " where is_spillover_day and late_events <> unique_events"
        ).fetchone()[0]

    assert health == (
        len(sim.messages),
        sim.unique_events,
        sim.kinds["duplicate"],
        stats.dead_lettered,
    )
    assert bad_spillover == 0
    # An order disappears only if every one of its events was dead-lettered.
    assert sim.orders - 5 <= orders <= sim.orders
    assert render(settings.warehouse_path, tmp_path / "report.html").stat().st_size > 5000


def test_incremental_run_with_late_events_matches_full_refresh(settings):
    """Load the stream in two halves; the incremental fct_orders must equal a rebuild."""
    messages = simulate(SIM).messages
    cutoff = messages[len(messages) // 2].arrival_ts
    no_lookback = ["--vars", "{incremental_lookback_minutes: 0}"]

    _publish(settings, [m for m in messages if m.arrival_ts < cutoff])
    pipeline.load(settings, "file")
    pipeline.transform(settings, ["build", *no_lookback])

    _publish(settings, [m for m in messages if m.arrival_ts >= cutoff])
    pipeline.load(settings, "file")
    pipeline.transform(settings, ["build", *no_lookback])
    incremental = _orders(settings)

    pipeline.transform(settings, ["build", "--full-refresh", *no_lookback])
    assert incremental == _orders(settings)
