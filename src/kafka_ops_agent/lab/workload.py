"""Synthetic producer and deliberately slow, manually committing consumer."""

import argparse
import json
import os
import signal
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from threading import Event
from typing import Any

BOOTSTRAP = "127.0.0.1:9092"
TOPIC = "demo-orders"
GROUP = "demo-consumer"
PARTITIONS = 3
STOP = Event()


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def write_json_line(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, sort_keys=True) + "\n")
        stream.flush()


def kafka_cluster_id() -> str:
    from confluent_kafka.admin import AdminClient

    metadata = AdminClient(
        {
            "bootstrap.servers": BOOTSTRAP,
            "client.id": "kafka-ops-a2-workload-identity",
        }
    ).list_topics(timeout=5)
    if not metadata.cluster_id:
        raise RuntimeError("fixture broker returned no Kafka cluster ID")
    return metadata.cluster_id


def ensure_topic() -> None:
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP, "client.id": "kafka-ops-a2-topic-setup"})
    topic = admin.list_topics(timeout=5).topics.get(TOPIC)
    if topic is None:
        future = admin.create_topics(
            [NewTopic(TOPIC, num_partitions=PARTITIONS, replication_factor=1)]
        )[TOPIC]
        try:
            future.result(timeout=10)
        except KafkaException as error:
            if error.args[0].code() != 36:  # TOPIC_ALREADY_EXISTS from a concurrent fixture start
                raise
        topic = admin.list_topics(topic=TOPIC, timeout=5).topics[TOPIC]
    if topic.error is not None or len(topic.partitions) != PARTITIONS:
        raise RuntimeError(f"fixture topic must have exactly {PARTITIONS} partitions")


def produce(records: int, records_per_second: float) -> None:
    from confluent_kafka import Producer

    if records < 1 or records > 10_000 or not 0 < records_per_second <= 100:
        raise ValueError("producer bounds are 1..10000 records and 0..100 records/second")
    ensure_topic()
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "kafka-ops-a2-producer"})
    output = Path(os.environ["KAFKA_OPS_LAB_DATA"])
    sent = delivered = 0

    def delivered_callback(error, message) -> None:
        nonlocal delivered
        if error is not None:
            raise RuntimeError(f"synthetic delivery failed: {error.str()}")
        delivered += 1

    started = time.monotonic()
    try:
        for sequence in range(records):
            if STOP.is_set():
                break
            event = {
                "fixture": "kafka-ops-a2",
                "sequence": sequence,
                "created_at": utc_now(),
            }
            producer.produce(
                TOPIC,
                value=json.dumps(event, separators=(",", ":")).encode(),
                partition=sequence % PARTITIONS,
                callback=delivered_callback,
            )
            producer.poll(0)
            sent += 1
            if sequence % 100 == 0:
                write_json_line(
                    output / "producer-progress.jsonl",
                    {
                        "observed_at": utc_now(),
                        "records_accepted": sent,
                    },
                )
            STOP.wait(1 / records_per_second)
    finally:
        remaining = producer.flush(10)
        write_json_line(
            output / "producer-progress.jsonl",
            {
                "observed_at": utc_now(),
                "records_accepted": sent,
                "records_delivered": delivered,
                "pending_at_shutdown": remaining,
                "duration_seconds": round(time.monotonic() - started, 3),
            },
        )
        if remaining or delivered != sent:
            raise RuntimeError("producer could not confirm delivery of every synthetic record")


def read_delay(path: Path) -> float:
    try:
        delay = json.loads(path.read_text(encoding="utf-8"))["delay_seconds"]
    except (OSError, KeyError, ValueError, TypeError) as error:
        raise RuntimeError("scenario control file is missing or invalid") from error
    if isinstance(delay, bool) or not isinstance(delay, (int, float)) or not 0 <= delay <= 0.5:
        raise RuntimeError("processing delay is outside the fixture limit of 0..0.5 seconds")
    return float(delay)


def consume(control_file: Path) -> None:
    from confluent_kafka import Consumer

    output = Path(os.environ["KAFKA_OPS_LAB_DATA"])
    cluster_id = kafka_cluster_id()
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP,
            "client.id": "kafka-ops-a2-consumer",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )
    last_metrics = time.monotonic()
    processed = 0
    last_offsets: dict[int, int] = {}
    consumer.subscribe([TOPIC])
    try:
        while not STOP.is_set():
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(f"fixture consumer failed: {message.error()}")
            delay = read_delay(control_file)
            deadline = time.monotonic() + delay
            while not STOP.is_set() and time.monotonic() < deadline:
                STOP.wait(min(0.05, deadline - time.monotonic()))
            if STOP.is_set():
                break
            consumer.commit(message=message, asynchronous=False)
            processed += 1
            last_offsets[message.partition()] = message.offset() + 1
            now = time.monotonic()
            if now - last_metrics >= 1:
                write_json_line(
                    output / "processing-progress.jsonl",
                    {
                        "observed_at": utc_now(),
                        "cluster_id": cluster_id,
                        "topic": TOPIC,
                        "group": GROUP,
                        "records_processed_total": processed,
                        "last_processed_offsets": {
                            str(key): value for key, value in last_offsets.items()
                        },
                    },
                )
                last_metrics = now
    finally:
        consumer.close()
        write_json_line(
            output / "processing-progress.jsonl",
            {
                "observed_at": utc_now(),
                "cluster_id": cluster_id,
                "topic": TOPIC,
                "group": GROUP,
                "records_processed_total": processed,
                "last_processed_offsets": {str(key): value for key, value in last_offsets.items()},
                "stopped": True,
            },
        )


def main() -> int:
    from confluent_kafka import KafkaException

    parser = argparse.ArgumentParser(description="A2 synthetic Kafka lab workload")
    commands = parser.add_subparsers(dest="command", required=True)
    produce_parser = commands.add_parser("produce")
    produce_parser.add_argument("--records", type=int, default=600)
    produce_parser.add_argument("--rate", type=float, default=20)
    consume_parser = commands.add_parser("consume")
    consume_parser.add_argument("--control-file", type=Path, required=True)
    args = parser.parse_args()

    def stop(signum, frame) -> None:
        STOP.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        if args.command == "produce":
            produce(args.records, args.rate)
        else:
            consume(args.control_file)
    except (OSError, RuntimeError, ValueError, KafkaException) as error:
        print(f"lab workload failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
