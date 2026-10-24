from __future__ import annotations

from datetime import timedelta

from pourstream.transport import FileConsumer, FileProducer, partition_for
from tests.conftest import NOW


def _produce(root, n=30, keys=("S-a", "S-b", "S-c", "S-d")):
    producer = FileProducer(root, "t", partitions=3)
    for i in range(n):
        producer.send(keys[i % len(keys)], f"m{i}".encode(), NOW + timedelta(seconds=i))
    producer.flush()
    producer.close()


def test_keys_always_map_to_the_same_partition_and_keep_order(tmp_path):
    _produce(tmp_path)
    messages = FileConsumer(tmp_path, "t", "g").poll_batch(1000)
    assert len(messages) == 30
    for key in ("S-a", "S-b"):
        mine = [m for m in messages if m.key == key]
        assert {m.partition for m in mine} == {partition_for(key, 3)}
        assert [m.timestamp for m in mine] == sorted(m.timestamp for m in mine)


def test_consumer_resumes_from_committed_offsets(tmp_path):
    _produce(tmp_path)
    first = FileConsumer(tmp_path, "t", "g")
    batch = first.poll_batch(10)
    first.commit(batch)

    resumed = FileConsumer(tmp_path, "t", "g").poll_batch(1000)
    assert len(resumed) == 20
    assert not {(m.partition, m.offset) for m in batch} & {(m.partition, m.offset) for m in resumed}


def test_uncommitted_messages_are_redelivered_after_a_crash(tmp_path):
    _produce(tmp_path)
    crashed = FileConsumer(tmp_path, "t", "g")
    batch = crashed.poll_batch(10)  # processed, but the process dies before commit

    replay = FileConsumer(tmp_path, "t", "g").poll_batch(1000)
    assert len(replay) == 30
    assert {(m.partition, m.offset) for m in batch} <= {(m.partition, m.offset) for m in replay}


def test_consumer_groups_are_independent(tmp_path):
    _produce(tmp_path)
    a = FileConsumer(tmp_path, "t", "a")
    a.commit(a.poll_batch(1000))
    assert len(FileConsumer(tmp_path, "t", "b").poll_batch(1000)) == 30
