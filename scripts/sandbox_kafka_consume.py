#!/usr/bin/env python3
"""Manually drive the Kafka control consumer in complete isolation.

No Elasticsearch, no SFTP, no pipeline: this script produces a few control
messages to a throwaway topic and then consumes them back through the *real*
``etl.control_consumer.ControlConsumer`` — the same class the daemon uses —
printing each decoded ``ControlMessage`` and committing its offset only after
"handling" it (exactly like ``etl.__main__``).

It also produces one deliberately malformed ("poison") record to demonstrate
that the consumer skips it (commits past it) instead of getting stuck.

Run against the isolation stack (``docker-compose.isolation.yml``)::

    docker compose -f docker-compose.isolation.yml up -d
    python scripts/sandbox_kafka_consume.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

# Run without `pip install -e .`: make `import etl...` resolve from src/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from confluent_kafka import Producer

from etl.config import KafkaConfig
from etl.control_consumer import ControlConsumer
from etl.logging_setup import configure_logging

BROKERS = "localhost:9092"
TOPIC = "sandbox.control"


def _produce() -> int:
    """Produce 2 valid control messages + 1 poison record. Returns valid count."""
    producer = Producer({"bootstrap.servers": BROKERS})
    valid = [
        {"job_doc_id": "job-A", "correlation_id": "demo-1"},
        {"job_doc_id": "job-B", "correlation_id": "demo-2"},
    ]
    # Single-partition topic preserves produce order, so offsets are 0,1,2.
    producer.produce(TOPIC, json.dumps(valid[0]).encode("utf-8"))  # offset 0: valid
    producer.produce(TOPIC, b"} not json {")                       # offset 1: poison
    producer.produce(TOPIC, json.dumps(valid[1]).encode("utf-8"))  # offset 2: valid
    producer.flush(10)
    print(f"produced {len(valid)} valid + 1 poison record(s) to {TOPIC!r}\n")
    return len(valid)


def main() -> int:
    # Structured JSON logs so the consumer's poison-skip warning is visible.
    configure_logging("INFO")

    expected = _produce()

    # A fresh group id each run + `auto.offset.reset=earliest` (set in
    # KafkaConfig.confluent_config) means we read from the start of the topic,
    # so re-runs always see every record rather than resuming past committed
    # offsets.
    cfg = KafkaConfig(
        bootstrap_servers=BROKERS,
        control_topic=TOPIC,
        group_id=f"sandbox-{int(time.time())}",
    )
    consumer = ControlConsumer(cfg)

    seen = 0
    deadline = time.time() + 20.0  # safety net so the demo never hangs

    def stop() -> bool:
        return seen >= expected or time.time() > deadline

    try:
        for ctrl, commit, _raw in consumer.iter_messages(poll_timeout_s=1.0, stop=stop):
            seen += 1
            print(
                f"consumed #{seen}: job_doc_id={ctrl.job_doc_id!r} "
                f"correlation_id={ctrl.correlation_id!r} "
                f"partition={ctrl.raw_partition} offset={ctrl.raw_offset}"
            )
            commit()  # ack ONLY after handling — exactly like the daemon
    finally:
        consumer.close()

    ok = seen == expected
    print(
        f"\nconsumed {seen}/{expected} valid message(s); "
        "the poison record was skipped and committed past."
    )
    print(f"RESULT: {'OK' if ok else 'TIMEOUT/MISMATCH'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
