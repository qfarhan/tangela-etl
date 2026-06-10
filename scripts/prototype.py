"""Functional prototype / sandbox for the ETL core.

This script lets you exercise the *core* of the pipeline in isolation, without
standing up Kafka or SFTP. It has two modes:

  * **offline** (default) — runs the pure transformation + CSV + integrity core
    (`transformer` -> `csv_writer` -> `validator`) on a small hard-coded sample.
    No Kafka, no Elasticsearch, no SFTP. This is the fastest loop for iterating
    on a `column_paths` mapping and *seeing* the flat CSV it produces.

  * **--live** — additionally pulls real documents from the local docker-compose
    Elasticsearch (the one seeded by ``scripts/seed.py``), proving connectivity,
    the job loader, pagination, and the full extract->transform->CSV chain end
    to end. It still writes the CSV locally; it does NOT touch Kafka or SFTP.

It deliberately calls the *real* project functions, so whatever you see here is
exactly what the daemon does internally — just driven by hand.

Run:
    # offline transformation sandbox (no infrastructure needed)
    python scripts/prototype.py

    # against the local ES stack:
    #   ./scripts/setup_local.sh && python scripts/seed.py
    python scripts/prototype.py --live

If you have not run ``pip install -e .``, this script still works: it adds the
project's ``src/`` directory to the import path automatically.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path
from typing import Any

# --- make `import etl...` work whether or not the package was pip-installed ---
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from etl.csv_writer import write_csv
from etl.errors import EtlError, RecordCountMismatch
from etl.models import CsvResult
from etl.transformer import get_by_path, iter_transformed
from etl.validator import validate_counts

# Sample documents shaped like the ones scripts/seed.py writes to ES, plus one
# deliberately "ragged" record (no customer, empty items) to demonstrate how
# missing paths resolve to an empty string instead of crashing the job.
SAMPLE_HITS: list[dict[str, Any]] = [
    {"order_id": "o-000", "customer": {"name": "Acme"},
     "totals": {"amount_cents": 100}, "items": [{"sku": "sku-0-a"}, {"sku": "sku-0-b"}]},
    {"order_id": "o-001", "customer": {"name": "Globex"},
     "totals": {"amount_cents": 200}, "items": [{"sku": "sku-1-a"}]},
    {"order_id": "o-002",  # missing "customer"; empty "items"
     "totals": {"amount_cents": 300}, "items": []},
]

# The transformation spec under test. Edit this and re-run to experiment.
COLUMNS = ["order_id", "customer", "amount", "first_sku"]
COLUMN_PATHS = {
    "order_id":  "order_id",            # same-name top-level field
    "customer":  "customer.name",       # nested object
    "amount":    "totals.amount_cents",  # nested object
    "first_sku": "items[0].sku",        # list index
}


def _banner(text: str) -> None:
    print(f"\n== {text} ==")


def transform_to_csv(
    hits: list[dict[str, Any]],
    columns: list[str],
    column_paths: dict[str, str],
    out_path: Path,
    *,
    job_id: str,
) -> CsvResult:
    """The reusable core seam: project hits to flat rows and stream them to a
    CSV (+ SHA256 sidecar). This is the exact chain `pipeline.run_one` uses."""
    rows = iter_transformed(hits, column_paths, columns, job_id=job_id)
    return write_csv(rows, columns, out_path)


def _report(result: CsvResult, expected: int) -> int:
    """Print the CSV, verify the sidecar, and validate the row count."""
    print(f"\nwrote {result.csv_path}")
    print(f"  rows   : {result.row_count}")
    print(f"  sha256 : {result.sha256_hex}")
    print(f"  sidecar: {result.sidecar_path}")

    _banner("CSV contents")
    print(result.csv_path.read_text(encoding="utf-8"), end="")

    _banner("Integrity check (recompute SHA256 and compare to sidecar)")
    recomputed = hashlib.sha256(result.csv_path.read_bytes()).hexdigest()
    sidecar_line = result.sidecar_path.read_text(encoding="utf-8").strip()
    ok_hash = recomputed == result.sha256_hex
    print(f"  recomputed == reported : {ok_hash}")
    print(f"  sidecar line           : {sidecar_line}")

    _banner("Count validation (ES _count vs CSV rows)")
    try:
        validate_counts(expected=expected, actual=result.row_count)
        print(f"  OK: expected {expected} == {result.row_count} rows")
    except RecordCountMismatch as e:
        print(f"  MISMATCH: {e}")
        return 1
    return 0 if ok_hash else 1


def run_offline(out: Path) -> int:
    _banner("Offline transformation sandbox (no infrastructure)")
    print(f"columns      : {COLUMNS}")
    print(f"column_paths : {COLUMN_PATHS}")

    _banner("get_by_path() spot-checks (note: missing paths -> empty string)")
    checks = [
        (0, "customer.name"),
        (0, "items[0].sku"),
        (2, "customer.name"),   # the ragged doc: no customer
        (2, "items[0].sku"),    # the ragged doc: empty items list
    ]
    for idx, path in checks:
        value = get_by_path(SAMPLE_HITS[idx], path)
        print(f"  hits[{idx}]  {path:20s} -> {value!r}")

    result = transform_to_csv(SAMPLE_HITS, COLUMNS, COLUMN_PATHS, out, job_id="prototype")
    return _report(result, expected=len(SAMPLE_HITS))


def run_live(args: argparse.Namespace, out: Path) -> int:
    _banner("Live mode: extract from the local Elasticsearch")
    # Imported lazily so offline mode never needs the ES client.
    from elasticsearch import Elasticsearch

    from etl.extractor import expected_count, iter_hits
    from etl.job_loader import load_job

    es = Elasticsearch(args.es_url)
    try:
        job = load_job(es, job_index=args.job_index, job_doc_id=args.job_doc_id)
    except EtlError as e:
        print(f"  could not load job '{args.job_doc_id}' from '{args.job_index}': {e}")
        print("  did you run ./scripts/setup_local.sh and python scripts/seed.py?")
        return 2

    print(f"  job_id       : {job.job_id}")
    print(f"  data_index   : {job.data_index}")
    print(f"  query        : {job.query}")
    print(f"  columns      : {job.columns}")
    print(f"  column_paths : {job.column_paths}")

    expected = expected_count(es, job.data_index, job.query)
    print(f"  _count       : {expected}")

    hits = list(iter_hits(es, job, page_size=args.page_size, keep_alive=args.keep_alive))
    print(f"  fetched      : {len(hits)} hits via PIT + search_after")

    result = transform_to_csv(hits, job.columns, job.column_paths, out, job_id=job.job_id)
    return _report(result, expected=expected)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true",
                        help="pull real documents from the local Elasticsearch")
    parser.add_argument("--out", type=Path, default=None,
                        help="output CSV path (default: /tmp/etl-prototype/<name>.csv)")
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--job-index", default="etl-jobs")
    parser.add_argument("--job-doc-id", default="daily-sales-export")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--keep-alive", default="1m")
    parser.add_argument("--verbose", action="store_true",
                        help="emit the pipeline's structured JSON logs")
    args = parser.parse_args(argv)

    if args.verbose:
        from etl.logging_setup import configure_logging
        configure_logging("INFO")

    default_name = f"{args.job_doc_id}.csv" if args.live else "offline.csv"
    out = args.out or (Path("/tmp/etl-prototype") / default_name)

    return run_live(args, out) if args.live else run_offline(out)


if __name__ == "__main__":
    raise SystemExit(main())
