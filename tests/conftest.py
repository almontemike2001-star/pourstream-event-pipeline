from __future__ import annotations

import os
import socket
from datetime import UTC, datetime
from pathlib import Path

import pytest

from pourstream.config import Settings

NOW = datetime(2026, 10, 1, 1, 0, tzinfo=UTC)


def _kafka_reachable(bootstrap: str) -> bool:
    host, _, port = bootstrap.partition(":")
    try:
        with socket.create_connection((host, int(port or 9092)), timeout=1):
            return True
    except OSError:
        return False


def pytest_collection_modifyitems(config, items):
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092")
    if _kafka_reachable(bootstrap):
        return
    skip = pytest.mark.skip(reason=f"no Kafka broker reachable at {bootstrap}")
    for item in items:
        if "kafka" in item.keywords:
            item.add_marker(skip)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        data_dir=tmp_path / "data",
        kafka_bootstrap=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"),
    )


def make_event(**overrides) -> dict:
    event = {
        "event_id": "8f14e45f-ceea-467a-9f8e-6e2b1c8d1a01",
        "event_type": "order_placed",
        "event_ts": "2026-10-01T00:30:00Z",
        "session_id": "S-1",
        "customer_id": "C00001",
        "store_id": "ST001",
        "channel": "app",
        "order_id": "O-1",
        "amount": "125.00",
    }
    event.update(overrides)
    return {k: v for k, v in event.items() if v is not None}
