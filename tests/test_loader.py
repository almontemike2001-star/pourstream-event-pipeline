from __future__ import annotations

import json
from datetime import date

import duckdb

from pourstream.generator import SimConfig, simulate
from pourstream.loader import Lake, process_batch, run_loader
from pourstream.transport import FileConsumer, FileProducer
from tests.conftest import NOW, make_event

SMALL = SimConfig(start_date=date(2026, 10, 1), days=1, sessions_per_store_day=30, seed=3)


def _bronze(lake_dir, table="order_events"):
    return f"read_parquet('{lake_dir}/bronze/{table}/*/*.parquet', hive_partitioning=true)"


def _publish(root, messages):
    producer = FileProducer(root, "t")
    for m in messages:
        producer.send(m.key, m.value, m.arrival_ts)
    producer.flush()
    producer.close()


def test_batch_is_split_between_bronze_and_dlq(tmp_path):
    from pourstream.transport import Message

    good = json.dumps(make_event()).encode()
    msgs = [
        Message("t", 0, 0, "S-1", good, NOW),
        Message("t", 0, 1, "S-1", b"{not json", NOW),
        Message("t", 0, 2, "S-1", json.dumps(make_event(store_id="x")).encode(), NOW),
    ]
    lake = Lake(tmp_path)
    accepted, rejected = process_batch(msgs, lake)
    assert accepted == 1
    assert rejected == {"invalid_json": 1, "schema_violation": 1}
    con = duckdb.connect()
    assert con.execute(f"select count(*) from {_bronze(tmp_path)}").fetchone()[0] == 1
    dlq = con.execute(f"select error_type, _offset from {_bronze(tmp_path, 'order_events_dlq')}")
    assert sorted(dlq.fetchall()) == [("invalid_json", 1), ("schema_violation", 2)]


def test_empty_lake_is_readable(tmp_path):
    Lake(tmp_path)
    con = duckdb.connect()
    for table in ("order_events", "order_events_dlq"):
        assert con.execute(f"select count(*) from {_bronze(tmp_path, table)}").fetchone()[0] == 0


def test_loader_drains_topic_and_accounts_for_every_message(tmp_path):
    sim = simulate(SMALL)
    _publish(tmp_path / "broker", sim.messages)
    stats = run_loader(
        FileConsumer(tmp_path / "broker", "t", "g"),
        Lake(tmp_path / "lake"),
        batch_size=500,
        idle_polls_before_exit=1,
    )
    assert stats.messages == len(sim.messages)
    assert stats.dead_lettered == sim.kinds["malformed"]
    assert stats.accepted == len(sim.messages) - sim.kinds["malformed"]


def test_replay_after_crash_duplicates_bronze_but_not_events(tmp_path):
    sim = simulate(SMALL)
    broker, lake_dir = tmp_path / "broker", tmp_path / "lake"
    _publish(broker, sim.messages)
    lake = Lake(lake_dir)

    crashed = FileConsumer(broker, "t", "g")
    process_batch(crashed.poll_batch(400), lake)  # written, never committed

    run_loader(FileConsumer(broker, "t", "g"), lake, batch_size=500, idle_polls_before_exit=1)

    rows, unique = (
        duckdb.connect()
        .execute(f"select count(*), count(distinct event_id) from {_bronze(lake_dir)}")
        .fetchone()
    )
    assert rows > len(sim.messages) - sim.kinds["malformed"]  # the replayed batch landed twice
    assert unique == sim.unique_events  # ...but no event is lost or invented
