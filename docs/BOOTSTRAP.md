# Bootstrap & Isolation Sandbox

**What this is.** A minimal sandbox for poking the two external subsystems —
**Elasticsearch** and **Kafka** — *on their own*, with nothing else running. No SFTP, no transformer,
no CSV writer, no daemon. You bring up just ES + Kafka in containers, then run small standalone scripts
that exercise exactly one subsystem at a time:

- **`scripts/sandbox_es_extract.py`** — calls the standalone `es_extract` package (PIT + `search_after`)
  against a throwaway index it seeds itself.
- **`scripts/sandbox_kafka_consume.py`** — produces a few control messages and consumes them back through
  the *real* `ControlConsumer`, manually triggering the consume path the daemon uses.

**Why isolate?** When something misbehaves end-to-end, you want to know *which* boundary is at fault.
This sandbox lets you confirm "ES extraction works" and "Kafka consume works" independently, before
wiring them together in the full pipeline. It's also the fastest way to learn each subsystem.

**How it relates to the rest of the repo:**

| | Full stack (`docker-compose.yml`) | Isolation sandbox (this doc) |
|---|---|---|
| Services | Kafka + ES + SFTP (+ optional Kibana/kafka-ui) | **Kafka + ES only** |
| Brought up by | `./scripts/setup_local.sh` | `docker compose -f docker-compose.isolation.yml up -d` |
| Purpose | end-to-end smoke test of the whole daemon | exercise one subsystem at a time |
| Scripts | `scripts/seed.py`, `python -m etl` | `scripts/sandbox_es_extract.py`, `scripts/sandbox_kafka_consume.py` |

> ⚠️ The sandbox binds the **same host ports** (`9200`, `9092`) as the full stack. Run **one stack at a
> time**. If `setup_local.sh`'s containers are up, `docker compose down` them first (or vice versa).

---

## Prerequisites

- **Docker** (Desktop or Engine) with the Compose plugin.
- **Python 3.10+** with the project dependencies. Either install the package once:
  ```bash
  python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
  ```
  …or just the two client libraries the sandbox scripts need:
  ```bash
  .venv/bin/pip install elasticsearch confluent-kafka
  ```
  Both sandbox scripts prepend `src/` to `sys.path`, so they import `es_extract` / `etl` **without**
  needing an editable install — but you do need `elasticsearch` and `confluent-kafka` available.

Commands below assume your venv is active (or prefix with `.venv/bin/`).

---

## 1. Bring up ES + Kafka in isolation

The sandbox compose file (`docker-compose.isolation.yml`, in the repo root) defines exactly two
services — single-node Elasticsearch with security off, and Kafka in KRaft mode (no ZooKeeper):

```yaml
# Isolation sandbox — ONLY Elasticsearch + Kafka, nothing else.
name: etl-sandbox

services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.13.4
    container_name: sandbox-elasticsearch
    ports:
      - "9200:9200"
    environment:
      discovery.type: "single-node"
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
    ulimits:
      memlock: { soft: -1, hard: -1 }
      nofile: { soft: 65536, hard: 65536 }
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9200/_cluster/health >/dev/null"]
      interval: 5s
      timeout: 5s
      retries: 30

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: sandbox-kafka
    ports:
      - "9092:9092"
    environment:
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
      KAFKA_NODE_ID: "1"
      KAFKA_PROCESS_ROLES: "broker,controller"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1"
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1"
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1"]
      interval: 5s
      timeout: 5s
      retries: 30
```

Bring it up and wait for both to report healthy:

```bash
docker compose -f docker-compose.isolation.yml up -d

# wait until both containers are "healthy"
docker compose -f docker-compose.isolation.yml ps
```

**Health checks you can run by hand:**

```bash
# Elasticsearch is up?
curl -s http://localhost:9200/_cluster/health?pretty

# Kafka broker reachable? (runs inside the container)
docker exec sandbox-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null \
  && echo "kafka OK"
```

ES can take ~20–40s on a cold first boot. If `curl` refuses the connection, give it a few seconds and
retry; the container healthcheck retries for ~2.5 minutes.

---

## 2. Exercise `es_extract` in isolation

The `es_extract` package depends only on the stdlib and a duck-typed Elasticsearch client — **no Kafka,
no `etl` imports**. The sandbox script seeds a throwaway index, runs `count` + `iter_hits`, prints the
streamed hits, cross-checks the total against `_count`, and deletes the index.

▶️ **Run it:**

```bash
python scripts/sandbox_es_extract.py
```

✅ **Expected output** (abridged):

```
es_extract.count() -> 7

streaming hits via PIT + search_after (page_size=2):
  [0] {"order_id": "o-000", "customer": {"name": "cust-0"}, "totals": {"amount_cents": 100}}
  ...
  [6] {"order_id": "o-006", "customer": {"name": "cust-0"}, "totals": {"amount_cents": 700}}

streamed 7; _count 7; match = True
RESULT: OK
```

📄 **The script** (`scripts/sandbox_es_extract.py`):

```python
#!/usr/bin/env python3
"""Call the standalone ``es_extract`` package in complete isolation.

No Kafka, no SFTP, no transformer, no CSV — just ``es_extract.count`` and
``es_extract.iter_hits`` (point-in-time + ``search_after``) against a throwaway
Elasticsearch index this script seeds and deletes itself. It cross-checks the
streamed hit total against ``_count`` and exits non-zero on a mismatch.

Run against the isolation stack (``docker-compose.isolation.yml``)::

    docker compose -f docker-compose.isolation.yml up -d
    python scripts/sandbox_es_extract.py

For a fuller, argument-driven harness (custom query, NDJSON dump, auth, full
envelope) see ``scripts/try_es_extract.py``.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

# Run without `pip install -e .`: make `import es_extract` resolve from src/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from elasticsearch import Elasticsearch

from es_extract import count, iter_hits

ES_HOSTS = "http://localhost:9200"
INDEX = "sandbox-es-extract"


def main() -> int:
    es = Elasticsearch(ES_HOSTS)

    # 1) Seed a throwaway index with 7 small docs (delete-then-create so the
    #    count is deterministic across re-runs).
    es.options(ignore_status=[400, 404]).indices.delete(index=INDEX)
    es.indices.create(index=INDEX)
    for i in range(7):
        es.index(
            index=INDEX,
            id=f"doc-{i}",
            document={
                "order_id": f"o-{i:03d}",
                "customer": {"name": f"cust-{i % 3}"},
                "totals": {"amount_cents": (i + 1) * 100},
            },
        )
    es.indices.refresh(index=INDEX)

    query: dict[str, Any] = {"match_all": {}}

    # 2) Ground truth via _count.
    total = count(es, INDEX, query)
    print(f"es_extract.count() -> {total}\n")

    # 3) Stream every hit. page_size=2 forces the multi-page PIT + search_after
    #    path (a 7-doc index pages 4 times: 2,2,2,1).
    print("streaming hits via PIT + search_after (page_size=2):")
    streamed = 0
    for src in iter_hits(es, INDEX, query, page_size=2):
        print(f"  [{streamed}] {json.dumps(src)}")
        streamed += 1

    # 4) Clean up the throwaway index.
    es.options(ignore_status=[400, 404]).indices.delete(index=INDEX)

    ok = streamed == total
    print(f"\nstreamed {streamed}; _count {total}; match = {ok}")
    print(f"RESULT: {'OK' if ok else 'MISMATCH'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

### Variations

- **Query an index you already have**, dump raw hits to NDJSON, show the full envelope, or pass auth —
  use the fuller, argument-driven harness already in the repo:
  ```bash
  python scripts/try_es_extract.py --seed --cleanup          # seed 25 docs, stream, delete
  python scripts/try_es_extract.py --index my-index \
      --query '{"range": {"ts": {"gte": "now-1d"}}}' --dump /tmp/hits.ndjson
  python scripts/try_es_extract.py --seed --full-envelope --limit 3   # includes _id
  ```
- **Copied `es_extract/` out on its own?** It still works with zero `etl` imports — the minimal call is
  just:
  ```python
  from elasticsearch import Elasticsearch
  from es_extract import count, iter_hits

  es = Elasticsearch("http://localhost:9200")
  q = {"match_all": {}}
  print(count(es, "my-index", q))
  for src in iter_hits(es, "my-index", q, page_size=500):
      ...  # each `src` is a hit's _source dict
  ```

---

## 3. Exercise the Kafka consumer in isolation

This drives the **real** `etl.control_consumer.ControlConsumer` — the same class the daemon uses — but
with no pipeline behind it. The script produces 2 valid control messages plus 1 deliberately malformed
("poison") record, then consumes them back, printing each decoded `ControlMessage` and committing only
after "handling" it. The poison record is skipped (committed past) so it can't wedge the loop.

▶️ **Run it:**

```bash
python scripts/sandbox_kafka_consume.py
```

✅ **Expected output** (the poison-skip appears as a JSON `ERROR` log line from the consumer):

```
produced 2 valid + 1 poison record(s) to 'sandbox.control'

{"ts": "...", "level": "ERROR", "logger": "etl.control_consumer", "msg": "poison control message at p=0 o=1: ...", ...}
consumed #1: job_doc_id='job-A' correlation_id='demo-1' partition=0 offset=0
consumed #2: job_doc_id='job-B' correlation_id='demo-2' partition=0 offset=2

consumed 2/2 valid message(s); the poison record was skipped and committed past.
RESULT: OK
```

Note `offset=2` for the second valid message: offset 1 was the poison record, which the consumer
committed past without yielding.

📄 **The script** (`scripts/sandbox_kafka_consume.py`):

```python
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
```

### Things to try

- **Produce more / different messages** then re-run to watch them stream back. Bump the count by editing
  `_produce`, or drop a `correlation_id` to see it decode as `None`.
- **Watch redelivery semantics.** Because each run uses a *fresh* `group_id`, it always replays from the
  start. Pin a fixed `group_id` (and remove the timestamp) to see committed offsets cause a re-run to
  consume *nothing* new — the at-least-once delivery model the daemon relies on.
- **Raw consume without `etl`** (if you only want to see confluent-kafka itself):
  ```python
  from confluent_kafka import Consumer
  c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "raw-demo",
                "auto.offset.reset": "earliest", "enable.auto.commit": False})
  c.subscribe(["sandbox.control"])
  msg = c.poll(timeout=5.0)
  print(None if msg is None else msg.value())
  c.close()
  ```
- **CLI peek** at the topic from inside the broker container:
  ```bash
  docker exec sandbox-kafka kafka-console-consumer \
      --bootstrap-server localhost:9092 --topic sandbox.control --from-beginning --timeout-ms 3000
  ```

---

## 4. Tear down

```bash
docker compose -f docker-compose.isolation.yml down -v      # -v also removes the volumes
```

`-v` gives you a clean slate next time (no leftover indices or topics). Both sandbox scripts already
clean up after themselves (the ES index is deleted; the Kafka topic is disposable), so you can also just
re-run them without tearing down.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `port is already allocated` on up | the full stack (`setup_local.sh`) is still running | `docker compose down` the other stack first — they share ports 9200/9092 |
| `curl: (7) Failed to connect` to ES | ES still booting (cold start is slow) | wait ~30s; `docker compose -f docker-compose.isolation.yml ps` should show `healthy` |
| ES container exits / OOMs | not enough memory for the JVM | raise Docker's memory limit, or lower `ES_JAVA_OPTS` heap |
| Kafka client hangs on connect | advertised listener mismatch | the sandbox advertises `localhost:9092`; connect from the host as `localhost:9092` (not the container name) |
| `sandbox_kafka_consume.py` prints `RESULT: TIMEOUT` | broker not ready when producing | confirm `kafka OK` from the health check above, then re-run |
| `ModuleNotFoundError: confluent_kafka` / `elasticsearch` | client libs not installed | `pip install elasticsearch confluent-kafka` (see Prerequisites) |

---

## Where to go next

- **Build the whole service test-first**, module by module: [`TUTORIAL.v4.md`](TUTORIAL.v4.md).
- **Read every line** of `es_extract` and the consumer: [`TUTORIAL.v3.md`](TUTORIAL.v3.md) §3–4, §17.
- **Run the full end-to-end stack** (adds SFTP + the daemon): `./scripts/setup_local.sh`, then
  `scripts/seed.py` and `python -m etl` (see [`TUTORIAL.v4.md`](TUTORIAL.v4.md) Phase G).
