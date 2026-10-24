"""Micro-batch loader: broker -> contract validation -> Parquet bronze lake + DLQ.

Delivery guarantee is at-least-once: a batch is written to the lake first and its offsets
are committed afterwards. A crash in between replays the batch, so bronze can contain the
same message twice. Silver (dbt ``stg_order_events``) deduplicates on ``event_id``, which
also absorbs duplicates that producers sent themselves.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pourstream.events import EventRejected, parse_event
from pourstream.transport import Consumer, Message

log = logging.getLogger(__name__)

_META = [
    ("_topic", pa.string()),
    ("_partition", pa.int32()),
    ("_offset", pa.int64()),
    ("_message_ts", pa.timestamp("us", tz="UTC")),
    ("_ingested_at", pa.timestamp("us", tz="UTC")),
    ("_batch_id", pa.string()),
]

BRONZE_SCHEMA = pa.schema(
    [
        ("schema_version", pa.int16()),
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("event_ts", pa.timestamp("us", tz="UTC")),
        ("session_id", pa.string()),
        ("customer_id", pa.string()),
        ("store_id", pa.string()),
        ("channel", pa.string()),
        ("order_id", pa.string()),
        ("item_sku", pa.string()),
        ("quantity", pa.int16()),
        ("amount", pa.decimal128(10, 2)),
        ("payment_method", pa.string()),
        ("cancel_reason", pa.string()),
        *_META,
    ]
)

DLQ_SCHEMA = pa.schema(
    [
        ("raw_value", pa.string()),
        ("error_type", pa.string()),
        ("error_detail", pa.string()),
        ("message_key", pa.string()),
        *_META,
    ]
)


class Lake:
    """Append-only Parquet lake, Hive-partitioned by ingestion date."""

    TABLES = {"order_events": BRONZE_SCHEMA, "order_events_dlq": DLQ_SCHEMA}

    def __init__(self, root: Path):
        self.root = root
        # A zero-row file per table keeps readers working before the first batch lands
        # (e.g. a DLQ that has never received a message).
        for table, schema in self.TABLES.items():
            placeholder = root / "bronze" / table / "ingest_date=1970-01-01" / "_empty.parquet"
            if not placeholder.exists():
                placeholder.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(schema.empty_table(), placeholder)

    def write(self, table: str, schema: pa.Schema, rows: list[dict], batch_id: str) -> Path | None:
        if not rows:
            return None
        ingest_date = rows[0]["_ingested_at"].date().isoformat()
        out_dir = self.root / "bronze" / table / f"ingest_date={ingest_date}"
        out_dir.mkdir(parents=True, exist_ok=True)
        final = out_dir / f"batch_{batch_id}.parquet"
        tmp = final.with_suffix(".parquet.tmp")
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), tmp, compression="zstd")
        tmp.replace(final)  # atomic: readers never see half-written files
        return final


@dataclass
class LoadStats:
    batches: int = 0
    messages: int = 0
    accepted: int = 0
    rejected: Counter[str] = field(default_factory=Counter)

    @property
    def dead_lettered(self) -> int:
        return sum(self.rejected.values())


def _meta(msg: Message, ingested_at: datetime, batch_id: str) -> dict:
    return {
        "_topic": msg.topic,
        "_partition": msg.partition,
        "_offset": msg.offset,
        "_message_ts": msg.timestamp,
        "_ingested_at": ingested_at,
        "_batch_id": batch_id,
    }


def process_batch(messages: list[Message], lake: Lake) -> tuple[int, Counter[str]]:
    ingested_at = datetime.now(UTC)
    batch_id = f"{ingested_at:%Y%m%dT%H%M%S%f}_{uuid.uuid4().hex[:8]}"
    good: list[dict] = []
    bad: list[dict] = []
    rejected: Counter[str] = Counter()
    for msg in messages:
        meta = _meta(msg, ingested_at, batch_id)
        try:
            event = parse_event(msg.value, arrived_at=msg.timestamp)
        except EventRejected as exc:
            rejected[exc.error_type] += 1
            bad.append(
                {
                    "raw_value": msg.value.decode("utf-8", errors="replace"),
                    "error_type": exc.error_type,
                    "error_detail": exc.detail[:1000],
                    "message_key": msg.key,
                    **meta,
                }
            )
            continue
        row = event.model_dump()
        row["event_id"] = str(row["event_id"])
        good.append({**row, **meta})
    lake.write("order_events", BRONZE_SCHEMA, good, batch_id)
    lake.write("order_events_dlq", DLQ_SCHEMA, bad, batch_id)
    return len(good), rejected


def run_loader(
    consumer: Consumer,
    lake: Lake,
    *,
    batch_size: int = 5000,
    poll_timeout_s: float = 2.0,
    idle_polls_before_exit: int | None = 3,
    max_batches: int | None = None,
) -> LoadStats:
    """Consume until the topic is drained (``idle_polls_before_exit`` empty polls in a row),
    or forever when that is ``None``."""
    stats = LoadStats()
    idle = 0
    while max_batches is None or stats.batches < max_batches:
        messages = consumer.poll_batch(batch_size, poll_timeout_s)
        if not messages:
            idle += 1
            if idle_polls_before_exit is not None and idle >= idle_polls_before_exit:
                break
            continue
        idle = 0
        started = time.perf_counter()
        accepted, rejected = process_batch(messages, lake)
        consumer.commit(messages)  # only after the lake write succeeded
        stats.batches += 1
        stats.messages += len(messages)
        stats.accepted += accepted
        stats.rejected.update(rejected)
        log.info(
            "batch %d: %d msgs, %d accepted, %d dead-lettered in %.2fs",
            stats.batches,
            len(messages),
            accepted,
            sum(rejected.values()),
            time.perf_counter() - started,
        )
    return stats
