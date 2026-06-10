# kafka-es-csv-sftp-etl

A long-lived Python service that replaces an Apache NiFi flow:

1. Consumes a Kafka **control topic**.
2. For each message, loads a job description from an **Elasticsearch** doc.
3. Paginates the target ES data index (point-in-time + `search_after`).
4. Projects each hit to a flat row via a dotted-path **column map**.
5. Streams a **CSV** + **SHA256 sidecar** to local staging.
6. Validates ES `_count` against the CSV row count (5 `_count` retries + 1 full re-extract).
7. Uploads CSV + sidecar via the system `sftp` binary (5 retries with exp backoff).
8. Only commits the Kafka offset after success.

## Install

```bash
python3 -m venv .venv
.venv/bin/pip install -e ".[dev]"
```

## Configure

Copy `.env.example` to `.env` and fill in the required values:

```bash
cp .env.example .env
```

Required env vars (no default):

```
KAFKA_BOOTSTRAP_SERVERS
KAFKA_CONTROL_TOPIC
KAFKA_GROUP_ID
ES_HOSTS                # comma-separated
ES_JOB_INDEX
SFTP_HOST  SFTP_USER  SFTP_KEY_PATH  SFTP_REMOTE_DIR  SFTP_KNOWN_HOSTS
```

Retry knobs (all optional):

```
RETRY_MAX_ATTEMPTS=5
RETRY_BACKOFF_BASE=1.0
RETRY_BACKOFF_CAP=30.0
RETRY_JITTER=0.25
```

Pagination: point-in-time + `search_after` (Elastic's recommended deep-pagination mechanism). Tune with `PAGE_SIZE` and `PIT_KEEP_ALIVE`.

## Job document shape

Each Kafka control message is a JSON object with at least `job_doc_id`:

```json
{ "job_doc_id": "daily-sales-export", "correlation_id": "abc-123" }
```

The referenced ES document in `ES_JOB_INDEX` looks like:

```json
{
  "job_id": "daily-sales-export",
  "data_index": "sales-2026-05",
  "query": { "range": { "ts": { "gte": "now-1d" } } },
  "column_paths": {
    "order_id": "order_id",
    "customer": "customer.name",
    "amount_cents": "totals.amount_cents",
    "items_first_sku": "items[0].sku"
  },
  "columns": ["order_id", "customer", "amount_cents", "items_first_sku"],
  "remote_filename": "exports/daily-sales-2026-05-26.csv"
}
```

`column_paths` is optional. Columns absent from the map fall back to a same-named top-level key in the hit's `_source`. Missing paths yield an empty string.

## Run

```bash
.venv/bin/python -m etl
```

`SIGINT` / `SIGTERM` triggers a graceful stop after the current job finishes.

## Tests

```bash
.venv/bin/pytest -q                        # 72 tests, no network needed
.venv/bin/pytest --cov=etl --cov-report=term-missing
```

Mocks cover Kafka / ES / SFTP — no Docker required.

## Local mock environment

A `docker compose` stack is bundled for end-to-end smoke tests. It runs Kafka (KRaft, no Zookeeper), Elasticsearch 8.x (security off), and an `atmoz/sftp` SFTP server with key-only auth. Kibana is included behind an opt-in profile.

```bash
# 1. Start Docker Desktop, then:
./scripts/setup_local.sh                # boots stack, generates SSH keys, captures known_hosts
cp .env.local .env                      # point the ETL at the local stack
.venv/bin/python scripts/seed.py        # creates ES job doc + 5 sample hits + 1 control message
.venv/bin/python -m etl                 # daemon picks up the control message
# In another shell: confirm the CSV landed.
ls local/sftp/upload/
sha256sum -c local/sftp/upload/daily-sales-2026-05-26.csv.sha256

# Optional UIs (both behind the `ui` profile):
#   Kibana    → http://localhost:5601    (browse ES indices, run queries)
#   Kafka UI  → http://localhost:8090    (topics, messages, consumer groups, lag)
# 8090 is used because 8080 is commonly taken (Airflow, etc.).
docker compose --profile ui up -d kibana kafka-ui

# Tear down (keeps generated keys / uploaded files):
./scripts/teardown_local.sh
# Wipe everything:
./scripts/teardown_local.sh --purge
```

What the stack exposes:

| Service          | Endpoint              | Notes                                         |
|------------------|-----------------------|-----------------------------------------------|
| Kafka            | `localhost:9092`      | KRaft mode, auto-create topics on             |
| Elasticsearch    | `http://localhost:9200` | security disabled (`xpack.security.enabled=false`) |
| SFTP             | `sftp://etl@localhost:2222` | key auth only; key at `local/keys/id_ed25519` |
| Kibana (opt)     | `http://localhost:5601` | only when started with `--profile ui`         |

Files created locally (gitignored — none of them are secrets, just generated for the mock):

```
local/keys/id_ed25519          # ETL client private key
local/keys/id_ed25519.pub
local/keys/known_hosts         # captured from the SFTP container's host key
local/sftp/ssh_host_keys/      # persistent SFTP host keys
local/sftp/etl_authorized_keys # public key copy mounted into the container
local/sftp/upload/             # where the ETL drops CSVs
```

Failure-mode smoke tests against the mock stack:

* **Count mismatch**: re-run `seed.py`, then before starting the ETL, delete a doc with
  `curl -X POST 'http://localhost:9200/sales-2026-05/_delete_by_query' -H 'Content-Type: application/json' -d '{"query":{"term":{"order_id":"o-002"}}}'`.
  The job will retry the count 5×, run one full re-extract, then raise `RecordCountMismatch`. Inspect the Kafka offset to see it didn't move:
  `docker exec etl-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group etl-local --describe`.
* **SFTP failure**: `docker stop etl-sftp`, restart the ETL, watch 5 retries with exp backoff, then `SftpUploadError`.
* **Redelivery**: kill the daemon (`Ctrl-C`) mid-job and restart — the same control message is consumed again.

## Failure-mode smoke tests

* Tamper with ES so `_count != rows`: job retries the count 5×, runs one full re-extract, then raises `RecordCountMismatch`. Offset is *not* committed.
* Block port 22 on the SFTP host: job retries 5× with exp backoff, then raises `SftpUploadError`. Offset is *not* committed.
* Restart the daemon: the same control message is redelivered (offset never moved).

## Layout

```
src/es_extract/          standalone ES extraction (deps: elasticsearch + stdlib only)
  pagination.py          PIT + search_after streaming generator
  extract.py             count() + one-call iter_hits()
  diagnostics.py         tee_to_ndjson / dump_to_ndjson
  errors.py              EsExtractError (injectable error_cls)
src/etl/
  __main__.py            entry-point: poll loop, commit on success
  config.py              env-var → Settings dataclass
  control_consumer.py    confluent_kafka.Consumer wrapper, manual commits
  job_loader.py          GET <job_doc_id> → JobSpec
  extractor.py           _count + PIT/search_after hits (wraps es_extract)
  transformer.py         dotted-path projection (no JOLT)
  csv_writer.py          streaming CSV + sha256 sidecar
  validator.py           5× _count retries → 1 full re-extract
  sftp_uploader.py       subprocess sftp -b … with retry decorator
  retry.py               generic exp-backoff + jitter
  pipeline.py            run_one(): orchestrates one job end-to-end
```

## Reusable ES extraction (`es_extract`)

The Elasticsearch extraction layer is a standalone package with **no dependency on
the rest of this project** — copy `src/es_extract/` or `pip install` this repo and use it
anywhere:

```python
from elasticsearch import Elasticsearch
from es_extract import count, iter_hits

es = Elasticsearch("http://localhost:9200")
q = {"match_all": {}}
print(count(es, "my-index", q))
for src in iter_hits(es, "my-index", q):       # PIT + search_after under the hood
    ...                       # each hit's _source; pass source_only=False for the full envelope
```

`etl`'s `extractor.py` is a thin wrapper over this package that pins failures to
`ElasticsearchQueryError`. To exercise `es_extract` on its own against a real ES —
optionally seeding a throwaway index first — run:

```bash
python scripts/try_es_extract.py --seed --cleanup
```

## Diagnostics: dump the raw hits

Set **`ES_RAW_DUMP_DIR`** to capture exactly what came out of Elasticsearch, before
transformation: each job streams its raw hits to `<ES_RAW_DUMP_DIR>/<job_id>.ndjson`
(one JSON object per line) as it runs. Unset by default — zero overhead when off.

## A note on JOLT

The original design referenced JOLT (the JSON-to-JSON transformer used in NiFi's `JoltTransformJSON`). There is no maintained Python port on PyPI, so we replaced JOLT with a simple dotted-path projection (`column_paths`). This covers every flat-CSV export shape we've seen. If a future job needs richer transformation (wildcards, conditionals), add a small Python pre-step in `transformer.py`.
