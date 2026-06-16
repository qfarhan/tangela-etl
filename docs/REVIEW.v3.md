# Project Review — v3 (Complete File Coverage & Design Decisions)

**What this document is.** A single, exhaustive review of the `kafka-es-csv-sftp-etl` repository:
**every file that ships is listed here**, each with its purpose, the **design decision** behind it, and
its current review status. It also adds a **Bootstrap & Live-Testing** section (the docker-compose mock
stack and the end-to-end smoke run), which the earlier reviews did not cover.

**Lineage.**
- **v1** — [`REVIEW.md`](REVIEW.md): the original errata/punch-list against `TUTORIAL.md` + `DESIGN.md`.
- **v2** — [`TUTORIAL.v2.md`](TUTORIAL.v2.md): the tutorial rewritten PIT-only, folding in v1's still-applicable findings.
- **v3** — *this file*: a comprehensive, file-by-file review + design-decision reference + the bootstrap/live-test section, carrying v1's findings forward and adding the adversarial findings from [`tests/test_adversarial.py`](../tests/test_adversarial.py).

**Verified current state (2026-06-16).**
- **72 unit tests** pass, `ruff` + `mypy --strict` clean, **~88%** line coverage (`pytest --cov=etl --cov=es_extract`).
- **12 adversarial probes** added (`tests/test_adversarial.py`): **3 pass, 9 fail by design** — each failure documents a real defect or unguarded boundary (see §D.3).
- **Live smoke test passed** against the docker-compose stack: a control message drove a real Kafka→ES(PIT)→CSV→validate→SFTP→commit cycle, lag returned to 0 (see §A.6).

---

## Severity legend

| Tag | Meaning |
|-----|---------|
| **P0** | Broken as written / behaves wrong. |
| **P1** | Misleading or a real capability gap. |
| **P2** | Polish, staleness, nits. |
| **✓** | Verified correct / no action. |
| **DEFERRED** | Acknowledged, intentionally not changed (rationale given). |

---

## Architecture in one screen

```
Kafka control topic ──(job_doc_id)──► ControlConsumer ──► run_one(ctrl, es, settings)
                                                              │
   ES job-index  ──GET job doc──► JobSpec ◄─────────── load_job
                                                              │
   ES data-index ──_count──► N (logged)                       │
                 ──PIT + search_after──► stream of _source ──► iter_transformed (dotted-path)
                                                              │     │
                                                       write_csv (stream + sha256 sidecar)
                                                              │
                                          validate_with_retry (5× _count, then 1 full re-extract)
                                                              │
                                          sftp -b  (CSV then .sha256, strict host key, 5 retries)
                                                              │
                                          __main__ commits the Kafka offset  ◄── only on success
```

**Control plane vs data plane.** Kafka carries only a *pointer* (`job_doc_id`); the heavy data flows
ES→CSV→SFTP. The job's *definition* lives in an ES document, so new exports are data, not code.

---

# §A — Bootstrap & Live Testing (new in v3)

The repo ships a complete local mock stack so the whole pipeline can be exercised end-to-end without
any cloud infrastructure. This section documents the moving parts, the design decisions behind them,
and the verified live run.

## A.1 The stack — [`docker-compose.yml`](../docker-compose.yml)

| Service | Image | Host port | Role |
|---|---|---|---|
| `etl-kafka` | `confluentinc/cp-kafka:7.6.1` | 9092 | Control topic broker |
| `etl-elasticsearch` | `elasticsearch:8.13.4` | 9200 | Job-index + data-index |
| `etl-sftp` | `atmoz/sftp:alpine` | 2222→22 | Delivery destination |
| `etl-kibana` *(profile `ui`)* | `kibana:8.13.4` | 5601 | Browse ES |
| `etl-kafka-ui` *(profile `ui`)* | `provectuslabs/kafka-ui` | 8090→8080 | Topics / groups / lag |

**Design decisions:**
- **Kafka in KRaft mode, no ZooKeeper.** One container instead of two; `CLUSTER_ID` is a fixed
  base64 UUID; `broker,controller` roles co-located. Lowest-friction single-node broker.
- **Elasticsearch single-node, `xpack.security.enabled=false`.** No TLS/passwords for a throwaway
  local cluster — keeps `.env.local` free of secrets. `ES_JAVA_OPTS=-Xms512m -Xmx512m` caps heap so it
  co-exists with other local containers.
- **`atmoz/sftp` with `etl::1001` (key-only, no password).** Exercises the *real* `sftp` binary and
  strict host-key path that production uses, not a mock.
- **Persistent host keys** are bind-mounted from `local/sftp/ssh_host_keys` so `known_hosts` stays
  valid across `down/up` — otherwise every restart would invalidate the captured host key.
- **UIs are opt-in behind `--profile ui`** so the default `up` stays lean. `kafka-ui` is on **8090**
  because 8080 is commonly taken (Airflow, etc.).
- **Healthchecks** on Kafka and ES let the setup script wait on readiness deterministically.
- **No named volumes for ES** → data is ephemeral; a `down -v` gives a clean slate every time.

## A.2 Bring-up — [`scripts/setup_local.sh`](../scripts/setup_local.sh)

Idempotent. Generates the ETL client `ed25519` keypair, generates persistent SFTP host keys, runs
`docker compose up -d`, waits for ES/SFTP/Kafka health, captures the SFTP host key into
`local/keys/known_hosts` via `ssh-keyscan`, and explicitly creates the `etl.control` topic.

> **Field note (observed during the live run).** On a cold first boot ES took **>60s** to answer,
> longer than the script's 60-attempt wait, so the script exited non-zero *after the containers were
> already up*. It is **idempotent** — re-running (or pre-waiting for ES health, then running) completes
> the remaining steps. A subsequent clean boot came up `green` in ~13s. Worth bumping the ES wait to
> ~120 attempts to avoid the spurious first-run failure.

## A.3 Config for local — [`.env.local`](../.env.local)

Copied to `.env` to point the ETL at the local stack (`localhost:9092` / `:9200` / `:2222`, key paths
under `local/keys/`). **Fixed in this pass:** removed the stale `PAGINATION_STRATEGY=scroll` and
`SCROLL_KEEP_ALIVE` lines left over from before the PIT-only refactor (config silently ignored them,
but they were misleading). Now mirrors `.env.example`'s PIT-only pagination block.

## A.4 Seed — [`scripts/seed.py`](../scripts/seed.py)

Indexes the `daily-sales-export` job document into `etl-jobs`, resets `sales-2026-05` with 5 sample
order docs (delete-then-create for deterministic counts), and produces **one** control message to
`etl.control`. Uses `refresh="wait_for"` / `indices.refresh` so the data is searchable before the
control message is consumed — avoiding a near-real-time race with the validator.

## A.5 Teardown — [`scripts/teardown_local.sh`](../scripts/teardown_local.sh)

`docker compose --profile ui down -v` (removes UI containers too, even if started separately).
`--purge` also wipes `local/keys` and `local/sftp`. Without `--purge`, the bind-mounted
`local/sftp/upload/` keeps previously delivered CSVs — *which is a foot-gun for verification* (see A.6).

## A.6 The verified live run

```bash
./scripts/setup_local.sh            # stack up, topic created, known_hosts written
cp .env.local .env
.venv/bin/python scripts/seed.py    # job doc + 5 docs + 1 control message
PYTHONUNBUFFERED=1 .venv/bin/python -m etl   # daemon
```

The daemon's structured log captured the full pipeline (abridged):

```
starting etl daemon (control_topic=etl.control)
loading job (job_doc_id=daily-sales-export, kafka_offset=0)
GET  /etl-jobs/_doc/daily-sales-export        [200]
POST /sales-2026-05/_count                    [200]   expected_count=5
POST /sales-2026-05/_pit?keep_alive=1m        [200]   ← PIT opened
POST /_search                                 [200]   ← page 1 (search_after)
POST /_search                                 [200]   ← page 2 (empty → done)
DELETE /_pit                                  [200]   ← PIT closed in finally
csv written rows=5 sha256=ddaafc87…7206
POST /sales-2026-05/_count                    [200]   counts validated rows=5
sftp invoking … StrictHostKeyChecking=yes BatchMode=yes -P 2222
upload complete remote=upload/daily-sales-2026-05-26.csv
job committed (job_doc_id=daily-sales-export)
```

**Verified outcomes:** delivered CSV (header + 5 rows matching the seed), `shasum -a 256 -c` → **OK**,
local staging copy present, consumer group `etl-local` **CURRENT=1 / END=1 / LAG=0** (offset committed).

**Two lessons recorded:**
1. **Stale-file foot-gun.** A first attempt "detected delivery" in ~1s — but that was a **leftover
   May-26 file** (teardown without `--purge` keeps `local/sftp/upload/`). The daemon had been stopped
   before its consumer even joined the group (group-join took ~9s). *Verification must clear the drop
   dir first* (or assert on a fresh mtime), which v3's run did.
2. **Buffering hides logs.** Redirecting daemon stdout to a file block-buffers it; on a SIGTERM kill only
   the first line had flushed. Run the daemon with `PYTHONUNBUFFERED=1` (or rely on the committed
   offset + delivered file) when capturing a smoke run.

## A.7 Failure-mode smokes (documented in README, not yet automated)

- **Count mismatch:** delete a data doc before the run → 5× `_count` retries + 1 re-extract → `RecordCountMismatch`, offset **not** committed.
- **SFTP down:** `docker stop etl-sftp` → 5 retries with backoff → `SftpUploadError`, offset **not** committed.
- **Redelivery:** kill mid-job → same control message reconsumed (offset never moved).

These are real, valuable tests; consider scripting them as a `make smoke` target so they run on demand.

---

# §B — Complete file-by-file coverage

Every shipped file is in one of the tables below. "LOC" is approximate. "Design decision" states *why*
the file is the way it is; "Status" flags any open finding (cross-referenced to §D).

## B.1 Packaging, config & tooling

| File | Purpose | Design decision | Status |
|---|---|---|---|
| [`pyproject.toml`](../pyproject.toml) | Build, deps, tool config | **src-layout** (`where=["src"]`) forces installed-package imports — no accidental cwd imports. Single source of truth for deps. `ruff` selects `E,F,I,B,UP,SIM,RUF` (ignore `E501`); **`mypy strict`** over *both* `etl` and `es_extract`; pytest `pythonpath=["src"]`. Console script `etl=etl.__main__:main`. ES pinned `>=8,<9` (the 9.x `body=` removal). | ✓ (description fixed to "dotted-path projection", R3) |
| [`requirements.txt`](../requirements.txt) | Runtime deps mirror | Convenience for pipelines expecting a requirements file; **pyproject remains source of truth**. | ✓ |
| [`requirements-dev.txt`](../requirements-dev.txt) | Dev/test deps mirror | `-r requirements.txt` + the `dev` group. Same source-of-truth note. | ✓ |
| [`.env.example`](../.env.example) | Documented config template | 12-factor: every var documented, required vs optional split, PIT-only pagination block, optional `ES_RAW_DUMP_DIR` diagnostic. | ✓ |
| [`.gitignore`](../.gitignore) | VCS hygiene | Ignores `.venv`, `.env`/`.env.local`, tool caches, and the **generated mock artifacts** `local/keys/` + `local/sftp/` (keys are throwaway, not secrets). | ✓ |
| `.claude/settings.local.json` | Local Claude Code harness settings | Editor/tooling config, **not part of the application**. Out of review scope; noted for completeness. | ✓ (n/a) |

## B.2 `src/etl/` — the application (15 modules)

| File | LOC | Purpose | Design decision | Status |
|---|---|---|---|---|
| [`__init__.py`](../src/etl/__init__.py) | 1 | Package marker | `__version__ = "0.1.0"`. | ✓ |
| [`__main__.py`](../src/etl/__main__.py) | 95 | Daemon: `python -m etl` | Poll loop; `ConfigError`→exit 2 **before** logging; signal handlers set a stop flag (graceful, never abort mid-job); **`break`, not `continue`, on `EtlError`** — cumulative offset commits mean advancing+committing the next message would silently swallow the failed one; commit **only after success**; `finally` closes consumer+es; heavy `Elasticsearch` import is **lazy** in `_build_es_client`. | ✓ — 0% *unit* coverage **by design** (integration-shaped; covered by the §A live smoke). |
| [`config.py`](../src/etl/config.py) | 177 | env → frozen `Settings` | Fail-fast: missing required → `ConfigError` at startup. Typed helpers `_get/_get_int/_get_float`. `load_dotenv(override=False)` so real env wins over `.env`. `PaginationConfig` is **PIT-only** post-refactor. | **P1** — no **range** check: `PAGE_SIZE=0`/negative accepted (§D.3 #4/#5). |
| [`control_consumer.py`](../src/etl/control_consumer.py) | 105 | Kafka control wrapper | `enable.auto.commit=false` + **manual synchronous commit**. `consumer_factory` is **injectable** (test seam). Decode failures → `ControlMessageError`; **poison messages committed past** (don't loop forever); null/tombstone values skipped+committed; `correlation_id` type-checked. | ✓ |
| [`csv_writer.py`](../src/etl/csv_writer.py) | 80 | Streaming CSV + sha256 | `_HashingWriter` **decorator** hashes bytes *as they're written* (one pass, any size). **Binary mode + `lineterminator="\n"`** → identical bytes/hash cross-platform. `extrasaction="ignore"`; `_stringify` for `None`/`bool`. | **P1/P2** — `write()` returns `len(s)` not bytes (§D.3 #1); nested value → Python `repr` not JSON (#3); **no CSV formula-injection guard** (#2). |
| [`errors.py`](../src/etl/errors.py) | 79 | Exception hierarchy | **Single `EtlError` base** → the daemon catches at one boundary; non-`EtlError` is allowed to crash. `RecordCountMismatch`/`RetryExhausted` carry structured context. | **P2 DEFERRED** — `TransformError` still has a **JOLT docstring + dead `jolt_op` field** (§3.2; JOLT was dropped). |
| [`extractor.py`](../src/etl/extractor.py) | 40 | Thin ES seam | Owns `_count` + the hit stream; delegates to `es_extract`, **pinning failures to `ElasticsearchQueryError`** via `error_cls` so they land in the `EtlError` boundary. Keeps the seam thin. | ✓ — 100% covered. |
| [`job_loader.py`](../src/etl/job_loader.py) | 56 | ES doc → `JobSpec` | **Validate-at-the-boundary**: `found` check, type-checks `query`/`columns`/`column_paths`/`remote_filename`, coerces `data_index`/`job_id`. Untrusted document becomes a typed, trusted `JobSpec`. | **P1** — accepts **empty `columns: []`** (vacuous `all(...)`); failure surfaces later in `write_csv` (§D.3 #6). |
| [`logging_setup.py`](../src/etl/logging_setup.py) | 47 | Structured JSON logs | One JSON object per line; merges `extra` (minus reserved keys) into the payload so `job_id`/`correlation_id`/attempt counts ride along without format strings. `json.dumps(default=str)`. | ✓ — ~96% covered. |
| [`models.py`](../src/etl/models.py) | 50 | Inter-stage data types | **Frozen dataclasses** (`ControlMessage`, `JobSpec`, `CsvResult`) — immutable values passed between pure stages; `ControlMessage` keeps `raw_partition/offset` for the exact commit. | ✓ |
| [`pipeline.py`](../src/etl/pipeline.py) | 125 | `run_one` orchestration | **Composition**: each stage is a function; `run_one` wires them as a **lazy stream** (`iter_hits → [tee] → iter_transformed → write_csv`). `_reextract` **closure** opens a fresh PIT (the previous is spent). Raw-dump tee is **opt-in** (`ES_RAW_DUMP_DIR`). **Does not commit** — IoC, the caller owns that. | ✓ — 100% covered. (`initial expected_count` is logged-only; validator re-queries — §3.6 redundant `_count`, DEFERRED.) |
| [`retry.py`](../src/etl/retry.py) | 113 | Exp-backoff + jitter | Generic `retry_call` + `@retry`; **injectable `sleeper`/`rng`** (instant deterministic tests); `wrap_final` option; `attempts<1` guard. | **P1** — `_compute_delay` does `base*(2**attempt)` **before** the cap → `OverflowError` at large attempt (§D.3 #9). |
| [`sftp_uploader.py`](../src/etl/sftp_uploader.py) | 101 | `sftp -b` delivery | **Subprocess over `paramiko`**: argv **list** (no `shell=True` → no injection); `StrictHostKeyChecking=yes` + operator `known_hosts` + `BatchMode=yes` + key auth; temp batch file; `timeout`; retried via `retry_call`. | **P1 DEFERRED** — **no destination atomicity**: `put` straight to the final name; sidecar-after-data ordering is the implicit "ready" signal but undocumented as a contract (§3.3). |
| [`transformer.py`](../src/etl/transformer.py) | 95 | Dotted-path projection | **No JOLT** (no maintained Python port) — a small path language (`a.b[0].c`) with `isinstance` type-guards turning ragged data into `""`. **Fail loud only on operator error** (empty configured path), tolerate missing data. | **P0/P1** — path `"."` returns the **whole document** (§D.3 #7); `_id` is **unreachable** post-`_source` reduction (§D.3 #8 / §3.1). |
| [`validator.py`](../src/etl/validator.py) | 90 | Two-tier count check | **Tier 1**: re-query `_count` up to N× with backoff (handles refresh races). **Tier 2**: one full re-extract via an **IoC callback**. Disagree after both → `RecordCountMismatch` with the full attempt log. Hand-rolled loop (retries on a *value*, not an exception). | ✓ |

## B.3 `src/es_extract/` — the standalone extraction package (5 modules)

Depends on **only** the `elasticsearch` client + stdlib; **zero `etl` imports** → copy-out / `pip install`
reusable. Own version `0.2.0`.

| File | LOC | Purpose | Design decision | Status |
|---|---|---|---|---|
| [`__init__.py`](../src/es_extract/__init__.py) | 40 | Public API | Re-exports `count`, `iter_hits`, `SearchAfterPagination`, `tee_to_ndjson`, `dump_to_ndjson`, `EsExtractError`; explicit `__all__`. Quick-start docstring. | ✓ |
| [`errors.py`](../src/es_extract/errors.py) | 16 | `EsExtractError` | The **injectable `error_cls`** philosophy: package stays free of any host hierarchy; a host injects its own type so failures land in *its* `except`. DI applied to the **error taxonomy**. | ✓ |
| [`pagination.py`](../src/es_extract/pagination.py) | 94 | `SearchAfterPagination` | PIT open → `search_after` pages (sort `_shard_doc`, `track_total_hits=False`) → **close PIT in `finally`** even on early abandonment. `pit_id` refreshed from each response. `source_only` via `_emit`; every ES call wrapped in `error_cls`; cleanup failure swallowed (logged) so it never masks the real error. | ✓ |
| [`extract.py`](../src/es_extract/extract.py) | 45 | `count` + `iter_hits` | `count()` = the `_count` ground truth; `iter_hits()` = one-call convenience constructing the strategy. Both take `error_cls`; `iter_hits` forwards `source_only`. | ✓ |
| [`diagnostics.py`](../src/es_extract/diagnostics.py) | 37 | NDJSON capture | `tee_to_ndjson` — a **generator** that yields each hit through while writing it; file lifetime bound to the generator (memory-bounded, flushes on abandon). `dump_to_ndjson` = eager variant returning a count. | ✓ |

## B.4 `tests/` — the suite (16 files, 72 + 12 tests)

| File | Tests | What it pins |
|---|---|---|
| [`conftest.py`](../tests/conftest.py) | fixtures | **Autouse `_isolate_dotenv`** (neutralizes `load_dotenv` for hermeticity), `settings`/`sample_job_spec`/`sample_hits`/`tmp_csv_dir` fixtures, `FakeMessage`. Pure-Python fakes — **no real Kafka/ES/SFTP**. |
| [`__init__.py`](../tests/__init__.py) | — | Package marker. |
| [`test_config.py`](../tests/test_config.py) | 4 | Defaults, missing-required → `ConfigError`, retry overrides, bad-integer. |
| [`test_control_consumer.py`](../tests/test_control_consumer.py) | 4 | Manual-commit config, decode+commit-after-ack, null-value skip, poison-commit-past. |
| [`test_csv_writer.py`](../tests/test_csv_writer.py) | 5 | Round-trip+sidecar, missing/None, special chars, empty-columns raise, zero-rows header-only. |
| [`test_es_extract.py`](../tests/test_es_extract.py) | 11 | `count` + error wrapping, PIT paging+close, `source_only=False` envelope, early-close cleanup, open-error-no-close, injected error cls, `iter_hits` convenience, tee/dump NDJSON. |
| [`test_extractor.py`](../tests/test_extractor.py) | 4 | `expected_count` + wrap, `iter_hits` via PIT, error→`ElasticsearchQueryError`. |
| [`test_job_loader.py`](../tests/test_job_loader.py) | 7 | Happy path, missing doc/fields, bad columns, ES error wrapped, default empty `column_paths`, bad `column_paths`. |
| [`test_logging.py`](../tests/test_logging.py) | 1 | `extra` fields → JSON payload. |
| [`test_models.py`](../tests/test_models.py) | 2 | `JobSpec` immutable, `RecordCountMismatch` context. |
| [`test_pipeline.py`](../tests/test_pipeline.py) | 5 | Golden path, mismatch-after-reextract raises, SFTP failure after retries, transient SFTP recovers, raw-dump NDJSON. |
| [`test_retry.py`](../tests/test_retry.py) | 8 | First-try/third-attempt, exhaust-raises-original, `wrap_final`, doesn't-catch-unrelated, decorator form, attempts>0, injected rng. |
| [`test_sftp_uploader.py`](../tests/test_sftp_uploader.py) | 6 | Batch quoting, strict-host-key argv, retries-then-succeeds, exhaust-raises, timeout, missing-binary. |
| [`test_transformer.py`](../tests/test_transformer.py) | 9 | Top/nested/list-index/missing/None paths, path-then-fallback, empty-path raises, stream rows, pure top-level. |
| [`test_validator.py`](../tests/test_validator.py) | 6 | Equal/unequal, first-match-no-reextract, recover-after-flaps, reextract-succeeds, final-mismatch raises. |
| [`test_adversarial.py`](../tests/test_adversarial.py) | 12 | **New.** Unexpected-input probes; **9 fail by design** documenting open findings, 3 pass (robust). See §D.3. |

**Why no Docker is needed for the suite:** every external boundary is a seam with a fake injected —
`es` is a parameter, `ControlConsumer` takes a `consumer_factory`, `subprocess.run` is monkeypatched,
`sleeper`/`rng` are injected, and `load_dotenv` is neutralized. This makes the suite fast and
deterministic — and is *also why* the §A live test matters: it's the only thing exercising real
consumer-group join/commit, real PIT paging, and the real `sftp` binary.

## B.5 `scripts/` — operational helpers (5 files)

| File | Purpose | Design decision |
|---|---|---|
| [`prototype.py`](../scripts/prototype.py) | Hand-drive the core | **offline** mode (transformer→csv→validator on hard-coded data, zero infra) for fast `column_paths` iteration; `--live` adds real ES. Calls the *real* project functions; self-bootstraps `src/` onto the path. |
| [`seed.py`](../scripts/seed.py) | Seed the mock stack | Job doc + 5 deterministic docs + 1 control message; `refresh=wait_for` to dodge NRT races. |
| [`setup_local.sh`](../scripts/setup_local.sh) | Boot the stack | Idempotent: keys, compose up, health waits, `known_hosts`, topic create. (See A.2 wait-timeout note.) |
| [`teardown_local.sh`](../scripts/teardown_local.sh) | Stop the stack | `down -v` (+ UI profile); `--purge` wipes generated keys/uploads. |
| [`try_es_extract.py`](../scripts/try_es_extract.py) | Exercise `es_extract` alone | Proves the package's `etl`-independence against a real ES (`--seed --cleanup`); falls back to a minimal local `JobSpec` if `etl` isn't importable. |

## B.6 `docs/` — documentation

| File | Purpose | Status |
|---|---|---|
| [`README.md`](../README.md) | Repo-root overview: pipeline steps, install/configure/run, job-doc shape, local-stack guide, failure-mode smokes, layout, `es_extract` reuse | ✓ updated PIT-only; "72 tests"; job-doc example fixed to `order_id` (R1/R2). |
| [`DESIGN.md`](DESIGN.md) | Theory & patterns (§1–30) | ✓ rewritten PIT-only; header/typo defects (D1–D3) fixed. |
| [`TUTORIAL.md`](TUTORIAL.md) | Build log (tickets) | ✓ PIT-only; the one stale "strategy" prose reference fixed. |
| [`TUTORIAL.v2.md`](TUTORIAL.v2.md) | Tutorial **v2** | ✓ v1 review findings folded in (T5/§3.3/§3.4/§3.5/N1/N2 + cross-links). |
| [`REVIEW.md`](REVIEW.md) | Review **v1** | ✓ banner marks Scroll-specific findings (T1/T2) moot post-refactor. |
| `REVIEW.v3.md` | Review **v3** | *this file*. |

---

# §C — Design decisions index (cross-cutting)

The decisions that recur across many files, each stated once with its rationale:

1. **Control plane vs data plane.** Kafka carries a pointer; ES/CSV/SFTP carry the data. Keeps messages
   tiny and the job definition in *data* (`job_loader`, `models.JobSpec`).
2. **Ports & adapters.** External systems sit behind thin seams (`control_consumer`, `extractor`,
   `sftp_uploader`) so the core is testable with fakes — and `es_extract` is fully extractable.
3. **One error boundary (`EtlError`).** Everything recoverable inherits from it; the daemon catches at a
   single point and keeps running; bugs crash loudly. `ConfigError` is the exception — fatal *before* the
   loop (exit 2).
4. **At-least-once + manual commit, `break`-not-`continue`.** Cumulative offsets mean a failed job must
   halt without committing so it's redelivered — the single most important correctness property.
5. **Generators everywhere for streaming.** `iter_hits`/`iter_transformed`/`write_csv`/`tee_to_ndjson`
   keep memory bounded to one row/page regardless of result size.
6. **Resource lifecycle in `finally`.** The PIT (and the NDJSON file) are released even when the consumer
   abandons iteration early; cleanup failures are logged, never raised (so they can't mask the real error).
7. **PIT + `search_after` only.** Scroll was removed; one stable-cursor mechanism, Elastic's recommended
   one, over a frozen view — no offset drift, no server-side scroll contexts.
8. **Injectable `error_cls`.** `es_extract` wraps failures in a caller-supplied type so it stays free of
   any host's error hierarchy — DI applied to error taxonomy.
9. **Validate at the boundary.** `job_loader` and `config` turn untrusted input into typed, trusted
   values immediately; downstream code assumes shape.
10. **Subprocess over library (SFTP).** The system `sftp` binary with an argv list (no shell), strict
    host-key checking, and key auth — fewer deps, a smaller attack surface than `paramiko`.
11. **Immutability.** Frozen dataclasses for all inter-stage values; nothing mutates shared state.
12. **Dependency injection for testability.** `es`, `consumer_factory`, `sleeper`, `rng`, and the
    re-extract callback are all injectable — which is why 72 tests need no network.
13. **Structured JSON logging.** One object per line; `extra` carries correlation/job context for free.
14. **End-to-end integrity.** A SHA256 sidecar is computed in the same pass as the CSV and delivered
    after it, so a consumer can verify the byte stream.

---

# §D — Findings carried forward + new

## D.1 v1 findings — applied

`T1/T2` (scroll lint) **mooted by the PIT-only refactor**; `T3` (`_keep_alive`) resolved by the rewrite;
`T4` (`test_models.py`/`test_logging.py`) **added** (both now exist); `D1–D3` (DESIGN header/typo) fixed;
`R1` (README `_id` example → `order_id`), `R2` (test count), `R3`/§3.2-metadata (`pyproject` description)
all fixed. README now states **PIT-only**, **72 tests**, and a correct job-doc example.

## D.2 v1 findings — still DEFERRED (code unchanged)

| Ref | Finding | Why still open |
|---|---|---|
| §3.1 / T5 | Document `_id` unreachable by projection | Doc-only fix taken (TUTORIAL.v2 caveat); `source_only=False` now offers an opt-in path at the `es_extract` layer, but `etl` still defaults to `_source`. |
| §3.2 | `TransformError` JOLT docstring + dead `jolt_op` | Pure cleanup, no behavior; tutorial snippet mirrors it so doc/code stay aligned. |
| §3.3 | SFTP destination not atomic | A behavior change (temp-then-rename / documented ordering) — left for a ticket. |
| §3.4 / §3.5 | `_HashingWriter.write` return value; nested→`repr` | Harmless today (csv ignores the return; flat exports don't hit nested). |
| §3.6 | Redundant initial `_count` | Cheap; the wording in DESIGN was softened to "logged for observability". |

## D.3 New adversarial findings — [`tests/test_adversarial.py`](../tests/test_adversarial.py)

12 unexpected-input probes; each asserts the *desirable* behavior, so a failure = a real gap.
**9 failed, 3 passed.**

| # | Probe | Result | Finding |
|---|---|---|---|
| 1 | `_HashingWriter.write` returns bytes | ❌ | Returns `len(s)` (chars), not UTF-8 byte count (§3.4). **P2** |
| 2 | CSV cell `=1+1` neutralized | ❌ | Written raw → **spreadsheet formula injection**. No guard. **P1 (security)** |
| 3 | Nested `{"a":1}` → JSON | ❌ | Emits Python `repr` `{'a': 1}` (§3.5). **P2** |
| 4 | `PAGE_SIZE=0` rejected | ❌ | Accepted; only int-parse checked. **P1** |
| 5 | `PAGE_SIZE=-5` rejected | ❌ | Accepted; no range validation. **P1** |
| 6 | Empty `columns: []` rejected at load | ❌ | Accepted; blows up later in `write_csv`. **P1** |
| 7 | Path `"."` yields `""` | ❌ | Returns the **whole document** into one cell. **P0** |
| 8 | Column→`"_id"` carries the id | ❌ | `_source`-only → `""` (§3.1). **P1** |
| 9 | `_compute_delay(attempt=2000)` capped | ❌ | `OverflowError` — cap applied after `2**attempt`. **P1** |
| 10 | Out-of-range list index | ✅ | Degrades to `""`. Robust. |
| 11 | Unicode CSV hash round-trips | ✅ | Hash over bytes; consistent. Robust. |
| 12 | Non-object `query` rejected | ✅ | `JobSpecError`. Robust. |

**Recommended fix order (cheap + high value):** #9 (`min(cap, base * 2**min(attempt, 30))`), #7 (treat an
empty token list as a miss → `""`), #4–#6 (range/empty checks in `config` + `job_loader`), #2 (prefix a
`'` on cells starting with `= + - @`). #1/#3/#8 are documented-but-deferred per §D.2.

---

# §E — Coverage ledger (proof every file is accounted for)

| Area | Files | Where reviewed |
|---|---|---|
| Packaging/config/tooling | `pyproject.toml`, `requirements.txt`, `requirements-dev.txt`, `.env.example`, `.gitignore`, `.claude/settings.local.json` | §B.1 |
| `src/etl/` | 15 modules (`__init__`, `__main__`, `config`, `control_consumer`, `csv_writer`, `errors`, `extractor`, `job_loader`, `logging_setup`, `models`, `pipeline`, `retry`, `sftp_uploader`, `transformer`, `validator`) | §B.2 |
| `src/es_extract/` | 5 modules (`__init__`, `errors`, `pagination`, `extract`, `diagnostics`) | §B.3 |
| `tests/` | 16 files (`__init__`, `conftest`, 13 module tests, `test_adversarial`) | §B.4 |
| `scripts/` | 5 (`prototype`, `seed`, `setup_local`, `teardown_local`, `try_es_extract`) | §B.5 |
| Docs | `README.md`, `docs/DESIGN.md`, `docs/TUTORIAL.md`, `docs/TUTORIAL.v2.md`, `docs/REVIEW.md`, `docs/REVIEW.v3.md` | §B.6 |
| Stack / bootstrap | `docker-compose.yml`, `.env.local`, the `local/` runtime tree (gitignored) | §A |

Every tracked path (`git ls-files`, excluding the gitignored `local/` runtime tree) plus the two new
untracked files (`docs/TUTORIAL.v2.md`, `tests/test_adversarial.py`) is represented above.

---

# Appendix — verification commands

```bash
# Unit suite + coverage (72 tests, no infra)
.venv/bin/pytest -q --cov=etl --cov=es_extract --cov-report=term-missing

# Adversarial probes (9 fail by design — they document the §D.3 findings)
.venv/bin/pytest tests/test_adversarial.py -v

# Lint + types (the project's own green-bar gate)
.venv/bin/ruff check . && .venv/bin/mypy

# Full live smoke (Docker)
./scripts/setup_local.sh && cp .env.local .env \
  && .venv/bin/python scripts/seed.py \
  && PYTHONUNBUFFERED=1 .venv/bin/python -m etl       # Ctrl-C after "job committed"
rm -f local/sftp/upload/* ; ls -l local/sftp/upload/  # clear stale, then confirm a FRESH drop
( cd local/sftp/upload && shasum -a 256 -c *.sha256 )
docker exec etl-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group etl-local                        # LAG should be 0
./scripts/teardown_local.sh                           # stop (add --purge to wipe local/)

# Exercise es_extract standalone against a real ES
.venv/bin/python scripts/try_es_extract.py --seed --cleanup
```
