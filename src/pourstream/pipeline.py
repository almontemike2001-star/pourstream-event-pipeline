"""Pipeline steps: produce -> load -> transform -> report."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from pourstream.config import DBT_DIR, Settings
from pourstream.generator import SimConfig, SimResult, simulate
from pourstream.loader import Lake, LoadStats, run_loader
from pourstream.transport import (
    Consumer,
    FileConsumer,
    FileProducer,
    KafkaConsumer,
    KafkaProducer,
    Producer,
)

log = logging.getLogger(__name__)


def make_producer(settings: Settings, transport: str) -> Producer:
    if transport == "kafka":
        return KafkaProducer(settings.kafka_bootstrap, settings.topic)
    return FileProducer(settings.file_broker_dir, settings.topic)


def make_consumer(settings: Settings, transport: str) -> Consumer:
    if transport == "kafka":
        return KafkaConsumer(settings.kafka_bootstrap, settings.topic, settings.consumer_group)
    return FileConsumer(settings.file_broker_dir, settings.topic, settings.consumer_group)


def produce(settings: Settings, transport: str, sim: SimConfig) -> SimResult:
    result = simulate(sim)
    producer = make_producer(settings, transport)
    for msg in result.messages:
        producer.send(msg.key, msg.value, msg.arrival_ts)
    producer.flush()
    if hasattr(producer, "close"):
        producer.close()
    log.info(
        "produced %d messages (%d unique events, %d orders) %s",
        len(result.messages),
        result.unique_events,
        result.orders,
        dict(result.kinds),
    )
    return result


def load(settings: Settings, transport: str, batch_size: int = 5000) -> LoadStats:
    consumer = make_consumer(settings, transport)
    try:
        stats = run_loader(consumer, Lake(settings.lake_dir), batch_size=batch_size)
    finally:
        consumer.close()
    log.info(
        "loaded %d messages in %d batches: %d accepted, %d dead-lettered %s",
        stats.messages,
        stats.batches,
        stats.accepted,
        stats.dead_lettered,
        dict(stats.rejected),
    )
    return stats


def transform(settings: Settings, args: Sequence[str] = ("build",)) -> None:
    """Run dbt against the DuckDB warehouse.

    dbt runs in a subprocess so it releases the DuckDB file before the report opens it.
    """
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    Lake(settings.lake_dir)  # make sure the source globs resolve
    dbt = Path(sys.executable).with_name("dbt")
    cmd = [str(dbt), *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    env = {**os.environ, "POURSTREAM_DATA_DIR": str(settings.data_dir)}
    if subprocess.run(cmd, env=env, check=False).returncode != 0:
        raise RuntimeError(f"dbt {' '.join(args)} failed")
