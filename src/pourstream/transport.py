"""Broker transports.

``kafka`` talks to Redpanda/Kafka through confluent-kafka. ``file`` is a tiny append-only,
partitioned log on disk with committed consumer offsets. It has the same semantics
(keyed partitioning, per-partition ordering, at-least-once delivery), so the whole
pipeline also runs offline and in CI without a broker.
"""

from __future__ import annotations

import json
import zlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol


@dataclass(frozen=True)
class Message:
    topic: str
    partition: int
    offset: int
    key: str | None
    value: bytes
    timestamp: datetime  # broker/producer timestamp = arrival time


class Producer(Protocol):
    def send(self, key: str, value: bytes, timestamp: datetime) -> None: ...
    def flush(self) -> None: ...


class Consumer(Protocol):
    def poll_batch(self, max_messages: int, timeout_s: float) -> list[Message]: ...
    def commit(self, messages: Iterable[Message]) -> None: ...
    def close(self) -> None: ...


def partition_for(key: str, partitions: int) -> int:
    return zlib.crc32(key.encode()) % partitions


def _next_offsets(messages: Iterable[Message]) -> dict[int, int]:
    nxt: dict[int, int] = {}
    for m in messages:
        nxt[m.partition] = max(nxt.get(m.partition, 0), m.offset + 1)
    return nxt


# --------------------------------------------------------------------------- file broker


class FileProducer:
    def __init__(self, root: Path, topic: str, partitions: int = 3):
        self.dir = root / topic
        self.dir.mkdir(parents=True, exist_ok=True)
        self.partitions = partitions
        self._handles = {
            p: (self.dir / f"partition-{p}.log").open("a", encoding="utf-8")
            for p in range(partitions)
        }

    def send(self, key: str, value: bytes, timestamp: datetime) -> None:
        record = {
            "key": key,
            "ts": timestamp.astimezone(UTC).isoformat(),
            "value": value.decode("utf-8", errors="surrogateescape"),
        }
        self._handles[partition_for(key, self.partitions)].write(json.dumps(record) + "\n")

    def flush(self) -> None:
        for fh in self._handles.values():
            fh.flush()

    def close(self) -> None:
        for fh in self._handles.values():
            fh.close()


class FileConsumer:
    def __init__(self, root: Path, topic: str, group: str):
        self.topic = topic
        self.dir = root / topic
        self.offsets_path = root / "_offsets" / f"{group}.{topic}.json"
        committed = json.loads(self.offsets_path.read_text()) if self.offsets_path.exists() else {}
        self._committed = {int(p): o for p, o in committed.items()}
        self._position = dict(self._committed)

    def _partitions(self) -> list[int]:
        return sorted(int(p.stem.split("-")[1]) for p in self.dir.glob("partition-*.log"))

    def poll_batch(self, max_messages: int, timeout_s: float = 0.0) -> list[Message]:
        batch: list[Message] = []
        for p in self._partitions():
            start = self._position.get(p, 0)
            with (self.dir / f"partition-{p}.log").open(encoding="utf-8") as fh:
                for offset, line in enumerate(fh):
                    if offset < start:
                        continue
                    if len(batch) >= max_messages:
                        break
                    rec = json.loads(line)
                    batch.append(
                        Message(
                            topic=self.topic,
                            partition=p,
                            offset=offset,
                            key=rec["key"],
                            value=rec["value"].encode("utf-8", errors="surrogateescape"),
                            timestamp=datetime.fromisoformat(rec["ts"]),
                        )
                    )
                    self._position[p] = offset + 1
        return batch

    def commit(self, messages: Iterable[Message]) -> None:
        self._committed.update(_next_offsets(messages))
        self.offsets_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.offsets_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._committed))
        tmp.replace(self.offsets_path)

    def close(self) -> None:
        pass


# --------------------------------------------------------------------------- kafka


def ensure_topic(bootstrap: str, topic: str, partitions: int = 3) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics([NewTopic(topic, num_partitions=partitions)])
    try:
        futures[topic].result(timeout=30)
    except Exception as exc:  # another producer may have created it concurrently
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise


class KafkaProducer:
    def __init__(self, bootstrap: str, topic: str):
        from confluent_kafka import Producer as _Producer

        ensure_topic(bootstrap, topic)
        self.topic = topic
        self._producer = _Producer(
            {"bootstrap.servers": bootstrap, "enable.idempotence": True, "linger.ms": 20}
        )

    def send(self, key: str, value: bytes, timestamp: datetime) -> None:
        while True:
            try:
                self._producer.produce(
                    self.topic,
                    key=key.encode(),
                    value=value,
                    timestamp=int(timestamp.timestamp() * 1000),
                )
                break
            except BufferError:
                self._producer.poll(0.5)  # local queue full: wait for deliveries
        self._producer.poll(0)

    def flush(self) -> None:
        remaining = self._producer.flush(30)
        if remaining:
            raise RuntimeError(f"{remaining} messages were not delivered")


class KafkaConsumer:
    def __init__(self, bootstrap: str, topic: str, group: str):
        from confluent_kafka import Consumer as _Consumer

        self._consumer = _Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group,
                "enable.auto.commit": False,  # commit only after the lake write succeeds
                "auto.offset.reset": "earliest",
            }
        )
        self._consumer.subscribe([topic])

    def poll_batch(self, max_messages: int, timeout_s: float) -> list[Message]:
        batch: list[Message] = []
        for msg in self._consumer.consume(num_messages=max_messages, timeout=timeout_s):
            if msg.error():
                raise RuntimeError(msg.error())
            key = msg.key()
            batch.append(
                Message(
                    topic=msg.topic(),
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=key.decode() if key else None,
                    value=msg.value() or b"",
                    timestamp=datetime.fromtimestamp(msg.timestamp()[1] / 1000, tz=UTC),
                )
            )
        return batch

    def commit(self, messages: Iterable[Message]) -> None:
        from confluent_kafka import TopicPartition

        messages = list(messages)
        if not messages:
            return
        topic = messages[0].topic
        offsets = [TopicPartition(topic, p, o) for p, o in _next_offsets(messages).items()]
        self._consumer.commit(offsets=offsets, asynchronous=False)

    def close(self) -> None:
        self._consumer.close()
