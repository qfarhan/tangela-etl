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
