"""Exercise the standalone ``es_extract`` package against a real Elasticsearch.

This harness lets you test ``es_extract`` *in isolation* — no Kafka, no SFTP,
no transformer, no CSV. It builds a ``JobSpec``, then drives only
``es_extract.count`` + ``es_extract.iter_hits`` (point-in-time + ``search_after``)
and cross-checks the streamed hit total against ``_count``.

``es_extract`` itself imports **nothing** from ``etl``; the ``JobSpec`` here is
just a convenient typed container for ``data_index`` + ``query``. If you copied
``src/es_extract/`` out on its own (without ``etl``), this script still runs —
it falls back to a minimal local ``JobSpec``.

Examples
--------
Seed a throwaway index with 25 sample docs, stream them back, then delete it::

    python scripts/try_es_extract.py --seed --cleanup

Point it at an index you already have and dump the raw hits to NDJSON::

    python scripts/try_es_extract.py --index my-index \
        --query '{"range": {"ts": {"gte": "now-1d"}}}' --dump /tmp/hits.ndjson

Show the full hit envelope (including ``_id``) instead of just ``_source``::

    python scripts/try_es_extract.py --seed --full-envelope --limit 3

With auth::

    python scripts/try_es_extract.py --hosts https://es:9200 \
        --es-user elastic --es-password "$ES_PASSWORD" --index my-index
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# --- make `import es_extract` / `import etl` work without `pip install -e .` ---
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from es_extract import count, iter_hits, tee_to_ndjson

# Prefer the real pipeline dataclass; fall back to a minimal stand-in so this
# script works even when es_extract has been copied out without the etl package.
try:
    from etl.models import JobSpec
except Exception:  # pragma: no cover - exercised only outside this repo
    from dataclasses import dataclass, field

    @dataclass(frozen=True)
    class JobSpec:  # type: ignore[no-redef]
        job_id: str
        data_index: str
        query: dict[str, Any]
        column_paths: dict[str, str] = field(default_factory=dict)
        columns: list[str] = field(default_factory=list)
        remote_filename: str = "unused.csv"


def _connect(args: argparse.Namespace) -> Any:
    from elasticsearch import Elasticsearch

    kwargs: dict[str, Any] = {}
    if args.api_key:
        kwargs["api_key"] = args.api_key
    elif args.es_user:
        kwargs["basic_auth"] = (args.es_user, args.es_password or "")
    return Elasticsearch([h.strip() for h in args.hosts.split(",") if h.strip()], **kwargs)


def _seed(es: Any, index: str, n: int) -> None:
    """(Re)create ``index`` with ``n`` small sample docs and refresh it."""
    es.options(ignore_status=[400, 404]).indices.delete(index=index)
    es.indices.create(index=index)
    for i in range(n):
        es.index(
            index=index,
            id=f"doc-{i}",
            document={
                "order_id": f"o-{i:03d}",
                "customer": {"name": f"cust-{i % 5}"},
                "totals": {"amount_cents": (i + 1) * 100},
                "items": [{"sku": f"sku-{i}-a"}, {"sku": f"sku-{i}-b"}],
            },
        )
    es.indices.refresh(index=index)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--hosts", default="http://localhost:9200",
                        help="comma-separated ES hosts (default: %(default)s)")
    parser.add_argument("--index", default="es-extract-tryout",
                        help="index to query (default: %(default)s)")
    parser.add_argument("--query", default='{"match_all": {}}',
                        help="ES query DSL as a JSON string (default: match_all)")
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--keep-alive", default="5m")
    parser.add_argument("--limit", type=int, default=10,
                        help="max hits to print (the rest are still streamed)")
    parser.add_argument("--full-envelope", action="store_true",
                        help="yield the whole hit (incl. _id) instead of just _source")
    parser.add_argument("--dump", default=None,
                        help="also tee the raw hits to this NDJSON path")
    parser.add_argument("--seed", action="store_true",
                        help="(re)create --index with sample docs before querying")
    parser.add_argument("--seed-count", type=int, default=25)
    parser.add_argument("--cleanup", action="store_true",
                        help="delete --index at the end (only with --seed)")
    parser.add_argument("--es-user", default=None)
    parser.add_argument("--es-password", default=None)
    parser.add_argument("--api-key", default=None)
    args = parser.parse_args(argv)

    try:
        query = json.loads(args.query)
    except json.JSONDecodeError as e:
        print(f"--query is not valid JSON: {e}", file=sys.stderr)
        return 2

    try:
        es = _connect(args)
    except ImportError:
        print("The 'elasticsearch' package is required: pip install elasticsearch",
              file=sys.stderr)
        return 2

    if args.seed:
        print(f"seeding {args.seed_count} sample docs into '{args.index}' ...")
        _seed(es, args.index, args.seed_count)

    # Build a JobSpec — the same shape the pipeline uses. es_extract only reads
    # `.data_index` and `.query`; the other fields are irrelevant here.
    job = JobSpec(
        job_id="try-es-extract",
        data_index=args.index,
        query=query,
        column_paths={},
        columns=[],
        remote_filename="unused.csv",
    )
    print(f"\nJobSpec.data_index = {job.data_index!r}")
    print(f"JobSpec.query      = {json.dumps(job.query)}")

    # --- es_extract in isolation: count, then stream every matching hit ---
    total = count(es, job.data_index, job.query)
    print(f"\nes_extract.count()  -> {total}")

    dump_path = Path(args.dump) if args.dump else None
    hits = iter_hits(
        es, job.data_index, job.query,
        page_size=args.page_size, keep_alive=args.keep_alive,
        source_only=not args.full_envelope,
    )
    if dump_path is not None:
        hits = tee_to_ndjson(hits, dump_path)

    print(f"\nstreaming via PIT + search_after (showing up to {args.limit}):")
    streamed = 0
    for hit in hits:
        if streamed < args.limit:
            print(f"  [{streamed}] {json.dumps(hit, default=str)}")
        streamed += 1
    if streamed > args.limit:
        print(f"  ... ({streamed - args.limit} more not shown)")

    if dump_path is not None:
        print(f"\nraw NDJSON written to {dump_path}")

    if args.cleanup and args.seed:
        es.options(ignore_status=[400, 404]).indices.delete(index=args.index)
        print(f"cleaned up index '{args.index}'")

    ok = streamed == total
    print(f"\nstreamed {streamed} hit(s); _count reported {total}")
    print(f"RESULT: {'OK' if ok else 'MISMATCH'} (streamed == _count: {ok})")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
