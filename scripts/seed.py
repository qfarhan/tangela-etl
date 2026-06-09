"""Seed the local mock stack with a sample job and produce one control message.

What this script does:
  1. Creates the ES job index and indexes one job document
     (``daily-sales-export``) describing the export.
  2. Creates the data index ``sales-2026-05`` and writes 5 sample order
     documents into it.
  3. Produces a single control message to ``etl.control`` referencing the
     job document.

Run after ``scripts/setup_local.sh``. Safe to re-run — uses upserts and
deletes the data index first so row counts stay deterministic.
"""

from __future__ import annotations

import json
import sys
import time

from confluent_kafka import Producer
from elasticsearch import Elasticsearch

ES_URL = "http://localhost:9200"
KAFKA_BROKERS = "localhost:9092"
JOB_INDEX = "etl-jobs"
DATA_INDEX = "sales-2026-05"
CONTROL_TOPIC = "etl.control"
JOB_DOC_ID = "daily-sales-export"

JOB_DOC = {
    "job_id": JOB_DOC_ID,
    "data_index": DATA_INDEX,
    "query": {"match_all": {}},
    "column_paths": {
        "order_id":  "order_id",
        "customer":  "customer.name",
        "amount":    "totals.amount_cents",
        "first_sku": "items[0].sku",
    },
    "columns": ["order_id", "customer", "amount", "first_sku"],
    # Path is relative to the etl SFTP user's home; atmoz/sftp guarantees
    # `upload/` exists, so dropping the file there avoids mkdir gymnastics.
    "remote_filename": "upload/daily-sales-2026-05-26.csv",
}


def _sample_doc(i: int) -> dict[str, object]:
    return {
        "order_id": f"o-{i:03d}",
        "customer": {"name": f"Customer-{i}"},
        "totals": {"amount_cents": (i + 1) * 100},
        "items": [{"sku": f"sku-{i}-a"}, {"sku": f"sku-{i}-b"}],
    }


def _wait_for_es(es: Elasticsearch, *, attempts: int = 60) -> None:
    for _ in range(attempts):
        try:
            es.cluster.health(wait_for_status="yellow", timeout="2s")
            return
        except Exception:
            time.sleep(1)
    raise SystemExit("elasticsearch did not become ready in time")


def main() -> int:
    es = Elasticsearch(ES_URL)
    _wait_for_es(es)

    # Job index + doc.
    if not es.indices.exists(index=JOB_INDEX):
        es.indices.create(index=JOB_INDEX)
    es.index(index=JOB_INDEX, id=JOB_DOC_ID, document=JOB_DOC, refresh="wait_for")
    print(f"indexed job doc id={JOB_DOC_ID}")

    # Data index — reset to keep counts deterministic across runs.
    if es.indices.exists(index=DATA_INDEX):
        es.indices.delete(index=DATA_INDEX)
    es.indices.create(index=DATA_INDEX)
    for i in range(5):
        es.index(index=DATA_INDEX, document=_sample_doc(i))
    es.indices.refresh(index=DATA_INDEX)
    count = es.count(index=DATA_INDEX, body={"query": JOB_DOC["query"]})["count"]
    print(f"seeded {DATA_INDEX} with {count} documents")

    # Control message.
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
    payload = json.dumps(
        {"job_doc_id": JOB_DOC_ID, "correlation_id": f"smoke-{int(time.time())}"}
    ).encode("utf-8")
    producer.produce(CONTROL_TOPIC, payload)
    producer.flush(10)
    print(f"produced control message to {CONTROL_TOPIC}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
