# Design & Implementation Guide

**Project:** `kafka-es-csv-sftp-etl` — a control-driven Extract/Transform/Load service that
replaces an Apache NiFi flow.

**Who this is for:** an engineer who wants to *understand* the system deeply, and who could
*reimplement it from scratch* — starting from a minimal viable product (MVP) and growing it into
a production service. Every section explains **why** a design choice was made before showing
**how** it is implemented. Read it top to bottom; each part assumes the ones before it.

---

## Table of contents

**Part I — Orientation**
1. What this service does and what it replaces
2. The core mental model: control plane vs. data plane
3. High-level architecture
4. The design principles that recur everywhere

**Part II — Setup & foundations**
5. Project layout and packaging
6. Configuration: fail-fast, env-driven, immutable
7. Errors: a hierarchy with a single catch boundary
8. Logging: structured JSON for machines and humans
9. The retry primitive: backoff, jitter, and an injectable clock

**Part III — The control plane (Kafka)**
10. Why Kafka triggers the work but does not carry it
11. Delivery semantics: at-least-once and manual offset commit
12. The `ControlConsumer`
13. The daemon loop and process lifecycle

**Part IV — The data source (Elasticsearch)**
14. Data-driven jobs: the job document pattern
15. The job loader: validation at the boundary
16. Deep pagination theory: why `from`/`size` breaks
17. Point-in-time + `search_after`: a stable cursor
18. The pagination generator: streaming + lifecycle
19. `es_extract`: a standalone, reusable extraction package
20. The extractor: a thin, honest seam

**Part V — Transformation & output**
21. From documents to rows: the projection problem
22. The transformer: dotted-path projection
23. Streaming CSV and the integrity sidecar

**Part VI — Trust & delivery**
24. Validation theory: why counts disagree, and the two-tier strategy
25. SFTP delivery: subprocess over library, and the security posture

**Part VII — Putting it together**
26. The orchestrator: `run_one`
27. A full trace of one control message

**Part VIII — Quality & evolution**
28. Testing strategy: seams, fakes, and fixtures
29. The local mock environment
30. From MVP to full product: an incremental roadmap

---

# Part I — Orientation

## 1. What this service does and what it replaces

The business need is mundane and extremely common: **periodically export a slice of data from a
search cluster to a CSV file, and deliver that file to a partner over SFTP.** Today this is done
in Apache NiFi using a `PaginatedJsonQueryElasticSearch` processor wired to downstream processors
that flatten JSON, write CSV, and push to SFTP.

NiFi is a fine tool, but a flow like this accrues problems over time:

- **It is hard to unit test.** NiFi flows are configured visually; there is no natural place to
  write a fast, deterministic test that says "given these five documents, assert exactly this CSV."
- **Error handling is implicit.** Failure relationships and retries are wired by hand on the
  canvas; the logic that decides "is this a transient failure I should retry, or a permanent one I
  should abort on?" lives in processor properties rather than in reviewable code.
- **Validation is ad hoc.** Confirming that the number of rows written equals the number of records
  the query matched is the kind of safety check that is easy to skip in a visual flow.
- **It is not version-controlled in a way engineers can reason about** the same way they reason
  about a Python module with a diff.

So we are rebuilding this single flow as a **small, typed, testable Python service**. The goals,
in priority order:

1. **Correctness you can prove.** The number of rows in the CSV must equal the number of documents
   the query matched — and the service must *fail loudly* if it does not.
2. **Testability.** Every component can be exercised in isolation with fakes, with no network and
   no Docker, in milliseconds.
3. **Explicit, reviewable behavior.** Retries, pagination, and error classification are code you
   can read in a pull request.
4. **Pluggability.** The pagination strategy (and, later, the data source itself) can change
   without rewriting the pipeline.

Keep these four goals in mind; nearly every design decision below traces back to one of them.

## 2. The core mental model: control plane vs. data plane

This is the single most important idea in the whole system. Internalize it and the rest follows.

There are two completely different roles that data infrastructure can play:

- The **control plane** answers *"what work should happen, and when?"* It carries small, infrequent
  messages that *trigger* work.
- The **data plane** answers *"where does the bulk payload live?"* It is the large, high-volume
  store that the work actually reads from or writes to.

In this system:

- **Kafka is the control plane.** A message arrives on a *control topic*. It is tiny — a JSON
  object with an ID and maybe a correlation ID. It does **not** contain the data to export. It is
  a doorbell, not a delivery truck.
- **Elasticsearch is the data plane.** It holds both the *job definitions* (documents describing
  *what* to export) and the *actual data* to be exported. This mirrors the NiFi flow, where ES was
  the source.

```json
// A control-plane message. Note: no business data, just a pointer.
{ "job_doc_id": "daily-sales-export", "correlation_id": "abc-123" }
```

Why separate them so strictly? Because it keeps each system doing what it is good at. Kafka is
superb at durable, ordered, replayable *event delivery* — perfect for "a job was requested."
Elasticsearch is superb at *querying large document sets* — perfect for "give me every order from
yesterday." If we crammed the data into Kafka messages we would lose ES's query power; if we
polled ES for "is there work to do?" we would lose Kafka's durability and replay. The control/data
split lets us use the right tool for each half.

A useful consequence: **the trigger is decoupled from the definition.** The same control message
(`{"job_doc_id": "daily-sales-export"}`) can be sent today and next week; what it *means* is stored
in Elasticsearch and can be edited without touching Kafka. Operations teams can re-run a job by
re-emitting one tiny message.

## 3. High-level architecture

A single long-lived process consumes the control topic and runs **one job per message**, strictly
sequentially. The flow for one message:

```
 Kafka control topic
        │  {"job_doc_id": "..."}
        ▼
 ControlConsumer ───────────► ControlMessage(job_doc_id, correlation_id, partition, offset)
        │
        ▼
 JobLoader (ES GET by id) ──► JobSpec(data_index, query, column_paths, columns, remote_filename)
        │
        ▼
 Extractor.expected_count (ES _count) ──► N   (how many docs the query matches, recorded up front)
        │
        ▼
 SearchAfterPagination.iter_hits (PIT + search_after) ──► stream of _source dicts
        │
        ▼
 Transformer.iter_transformed (dotted-path projection) ──► stream of flat {column: value} dicts
        │
        ▼
 CsvWriter.write_csv (streaming + incremental SHA256) ──► file.csv + file.csv.sha256, row_count
        │
        ▼
 Validator.validate_with_retry (re-query _count vs row_count; retry; then 1 full re-extract) ──► OK
        │
        ▼
 SftpUploader.upload (sftp -b batch, key auth, retry) ──► csv + sidecar delivered
        │
        ▼
 commit Kafka offset   ◄── ONLY on full success
```

Two things to notice already, because they are load-bearing:

1. **The offset is committed only at the very end.** If anything fails before that, the message is
   *not* acknowledged, so it will be redelivered. This is how we get at-least-once processing
   without a database or a saga.
2. **Everything between "stream of hits" and "write CSV" is a generator pipeline.** We never hold
   the whole result set in memory. A 2-million-row export streams through the same code path as a
   5-row export. This is a deliberate choice we will return to.

## 4. The design principles that recur everywhere

Before diving into modules, here are the recurring ideas. When you see them again in context they
will already feel familiar.

**(a) Fail-fast configuration.** All configuration is read and validated *once*, at process start.
If a required value is missing, the process refuses to start with a clear error. We never discover
a misconfiguration halfway through a job.

**(b) Ports and adapters (a.k.a. hexagonal architecture), lightly applied.** The core logic
(transform, CSV, validate) does not import Kafka, Elasticsearch, or `subprocess`. The integrations
live at the edges and are injected in. This is *why* the core is testable without infrastructure.

**(c) Dependency injection over global singletons.** The Elasticsearch client, the Kafka consumer
factory, the `sleeper` (clock), and the random number generator are all passed in as arguments.
Tests substitute fakes; production passes the real thing. There is no hidden global state to reset
between tests.

**(d) Generators and streaming.** Extraction and transformation are lazy iterators. Memory use is
bounded by one page of results, not by the size of the export.

**(e) Idempotency and at-least-once.** Because a message can be redelivered, every job must be safe
to run more than once. Writing to a deterministic local path and overwriting it, then re-uploading,
is idempotent: re-running produces the same file.

**(f) Fail-loud validation.** The system would rather abort and redeliver than deliver a CSV that
silently dropped rows. The validator is the conscience of the pipeline.

**(g) A single error boundary.** All recoverable, job-scoped failures inherit from one base
exception (`EtlError`). The daemon catches exactly that type at one place. Anything that is *not*
an `EtlError` (a programming bug, an import error) is allowed to crash the process, because crashing
is the correct response to a bug.

---

# Part II — Setup & foundations

## 5. Project layout and packaging

```
python-kafka/
├── pyproject.toml            # dependencies, tool config, console entry point
├── README.md                 # quickstart + runbook
├── docker-compose.yml        # local mock stack (Kafka + ES + SFTP)
├── .env.example / .env.local # configuration templates
├── scripts/                  # setup_local.sh, teardown_local.sh, seed.py
├── src/
│   └── etl/                  # the package
│       ├── __main__.py       # entry point: `python -m etl`
│       ├── config.py
│       ├── errors.py
│       ├── models.py
│       ├── logging_setup.py
│       ├── retry.py
│       ├── control_consumer.py
│       ├── job_loader.py
│       ├── extractor.py
│       ├── transformer.py
│       ├── csv_writer.py
│       ├── validator.py
│       ├── sftp_uploader.py
│       └── pipeline.py
└── tests/                    # one test module per source module
```

**Why a `src/` layout?** Putting the package under `src/` rather than at the repo root prevents a
subtle and common bug: with a flat layout, `import etl` can accidentally resolve to the *source
tree* even when you meant to test the *installed* package. With `src/`, the only way to import `etl`
is to install it (`pip install -e .`), which means your tests exercise the same import path your
users get. It forces honesty about packaging.

**The entry point.** `pyproject.toml` declares:

```toml
[project.scripts]
etl = "etl.__main__:main"
```

This means after install, both `etl` and `python -m etl` run `etl.__main__.main()`. Having a real
entry point (rather than a loose script) is the difference between "a folder of files" and "an
installable, runnable service."

**Tooling.** `ruff` (lint + format), `mypy --strict` (types), and `pytest` (tests) are configured in
`pyproject.toml`. The minimum Python is **3.10**, chosen because the code uses modern typing syntax
(`str | None`, `dict[str, str]`) freely. Runtime dependencies are deliberately minimal:
`confluent-kafka`, `elasticsearch`, and `python-dotenv`. There is no web framework, no ORM, no
paramiko. Every dependency earns its place.

> **MVP note.** For an MVP you could start with a single `main.py` and no packaging. But adopt the
> `src/` layout and an entry point *early* — retrofitting packaging onto a pile of scripts is more
> painful than starting with it, and it is what makes the test strategy in Part VIII possible.

## 6. Configuration: fail-fast, env-driven, immutable

**The theory.** Configuration is the seam between your code and the environment it runs in (dev,
staging, prod). Three properties make configuration safe:

1. **Externalized.** Config comes from the environment, not hard-coded. The *same artifact* runs in
   every environment; only the env differs. (This is the "Config" factor of the Twelve-Factor App
   methodology.)
2. **Validated once, at the edge.** Read and check everything at startup. A process that starts
   successfully is a process whose config is known-good. Discovering a missing SFTP key after you
   have already extracted two million rows is a waste and a hazard.
3. **Immutable thereafter.** Once loaded, config never changes. Immutability removes a whole class
   of "who mutated this setting?" bugs.

**The implementation.** `config.py` defines a set of **frozen dataclasses** — `KafkaConfig`,
`EsConfig`, `PaginationConfig`, `RetryConfig`, `SftpConfig`, and the top-level `Settings` that holds
them all. `frozen=True` makes instances immutable: any attempt to assign to a field raises.

Loading is done by one function:

```python
def load_settings(*, dotenv_path: str | None = None) -> Settings: ...
```

It does three things, in order:

1. Calls `load_dotenv()` so that a local `.env` file populates `os.environ` in development. In
   production there is no `.env`; the env is set by the orchestrator (Kubernetes, systemd, etc.).
   `override=False` means real environment variables always win over the file.
2. Reads each value through small typed helpers — `_get` (string, with a `required=True` option),
   `_get_int`, `_get_float` — that raise `ConfigError` on a missing-required or malformed value.
3. Coerces and range-checks values (e.g., `PAGE_SIZE` must parse as an integer) and
   constructs the frozen dataclasses.

The helper pattern is worth internalizing:

```python
def _get(key: str, default: str | None = None, *, required: bool = False) -> str:
    val = os.environ.get(key, default)
    if required and (val is None or val == ""):
        raise ConfigError(f"missing required env var: {key}")
    return val if val is not None else ""
```

Note the design choices: required-but-missing is an *error*, not a silent empty string; and the
function never returns `None`, so callers do not have to litter the code with `if x is None`. The
typed variants (`_get_int`, `_get_float`) wrap parsing in a `try/except` and re-raise as
`ConfigError` with the offending value in the message — so an operator who typed `PAGE_SIZE=lots`
gets *"PAGE_SIZE must be an integer, got: 'lots'"* at startup, not a `ValueError` deep in a loop.

One small but instructive detail: `KafkaConfig` has a method `confluent_config()` that returns the
exact dict the `confluent_kafka.Consumer` constructor expects, including the non-negotiable
`"enable.auto.commit": "false"`. Putting that translation *on the config object* keeps the consumer
code clean and makes the "we always disable auto-commit" decision impossible to forget.

> **A real bug we fixed here, and the lesson.** `load_dotenv()` searches *up the directory tree* for
> a `.env` file. During local smoke testing we run `cp .env.local .env`, which leaves a real `.env`
> in the repo. The config *unit tests* then accidentally picked it up — repopulating variables a
> test had deliberately removed — and failed. The fix was to neutralize `load_dotenv` in tests (an
> autouse fixture). **Lesson:** any function that reads ambient state (the filesystem, the clock,
> the environment) is a hidden input. Make it injectable or neutralizable, or your tests are not
> hermetic.

## 7. Errors: a hierarchy with a single catch boundary

**The theory.** Exceptions are a classification system. The question every `except` clause implicitly
asks is: *"is this the kind of failure I know how to handle here?"* If your exceptions are all
`Exception`, you cannot answer that question without string-matching messages, which is brittle. A
well-designed hierarchy lets you catch exactly the right granularity.

**The implementation.** `errors.py` defines one base class and a flat set of subclasses:

```
EtlError                       # base: every recoverable, job-scoped failure
├── ConfigError                # startup misconfiguration
├── ControlMessageError        # undecodable / malformed Kafka message
├── JobSpecError               # the ES job document is missing/malformed
├── ElasticsearchQueryError    # any ES request failure (count, search, PIT)
├── TransformError             # projecting a hit failed (carries job_id, hit_id)
├── CsvWriteError              # local CSV/sidecar write failed
├── RecordCountMismatch        # validation failed (carries expected, actual, attempts[])
├── SftpUploadError            # sftp exited non-zero / timed out
└── RetryExhausted             # optional wrapper carrying the final exception + attempt count
```

The key architectural decision: **the daemon catches `EtlError` and nothing more specific at the
loop boundary.** Inside the pipeline, individual stages raise their specific subclass. The daemon
does not need to know the difference between a `TransformError` and an `SftpUploadError` to decide
its action — both mean "this job failed; do not commit the offset." Meanwhile, a `KeyError` from a
genuine bug is *not* an `EtlError`, so it is not caught — it crashes the process, which is exactly
what you want for a bug (crash, get paged, fix the bug; don't silently skip).

Some subclasses carry **structured context**, not just a message. `RecordCountMismatch` carries
`expected`, `actual`, and the full `attempts` history; `TransformError` carries `job_id` and
`hit_id`. This means a failure can be diagnosed from the exception object alone, without re-reading
logs or the source document.

> **MVP note.** Even in an MVP, define at least `EtlError` and one catch point. The temptation is to
> "just let it crash for now," but the moment you have a long-lived consumer, you need to decide
> per-failure whether to keep running or stop — and that decision is exactly what a base class plus
> one `except` gives you.

## 8. Logging: structured JSON for machines and humans

**The theory.** Logs have two audiences: humans reading them during an incident, and machines
(log aggregators, dashboards, alerts) querying them. Free-text logs serve the first audience poorly
under load and the second audience not at all. **Structured logs** — one JSON object per line, with
named fields — serve both: a human can read them, and a tool can filter `job_id == "daily-sales"`.

**The implementation.** `logging_setup.py` provides a `JsonFormatter` and a `configure_logging(level)`
function. Each log line becomes a JSON object with `ts`, `level`, `logger`, and `msg`, plus any
**extra fields** the caller attached:

```python
_log.info("csv written rows=%d sha256=%s", n, digest, extra=log_extra)
# → {"ts": "...", "level": "INFO", "logger": "etl.pipeline",
#    "msg": "csv written rows=5 sha256=ab12...", "job_id": "daily-sales-export",
#    "correlation_id": "abc-123", "kafka_offset": 42, ...}
```

The formatter merges `record.__dict__` (minus the reserved `LogRecord` attributes) into the payload.
This is the **structured-context pattern**: instead of formatting context into the message string,
you pass it as `extra={...}` and let the formatter render it as queryable fields. The pipeline builds
a `log_extra` dict once per job (`job_doc_id`, `correlation_id`, `kafka_partition`, `kafka_offset`,
later `job_id` and `data_index`) and threads it through every log call, so *every* line for a given
job is tagged identically. During an incident you filter on `correlation_id` and see the whole story.

A security note that the formatter quietly enforces: because fields are run through `json.dumps`,
any newlines or control characters in user-controlled values (like `job_doc_id`) are escaped. That
prevents **log forging**, where an attacker injects a fake log line via a crafted field.

> **Production note.** The formatter writes to stdout. That is correct for containers: the
> orchestrator captures stdout and ships it. Do *not* have the application manage log files — that is
> the platform's job.

## 9. The retry primitive: backoff, jitter, and an injectable clock

We cover retries *now*, before Kafka/ES/SFTP, because all three build on this one primitive. Getting
the abstraction right here pays off three times.

**The theory.** Networked operations fail transiently — a connection resets, a node is briefly
busy. Retrying is the standard remedy, but naive retries make things *worse*:

- **Retrying immediately** hammers a struggling service and can turn a blip into an outage.
- **Retrying on a fixed schedule** synchronizes all your clients — if a service hiccups, every
  client retries at exactly the same moments, producing a "thundering herd" that re-creates the
  load spike. This is the famous failure mode that motivates jitter.

The fix is **exponential backoff with jitter**: wait longer after each failure (1s, 2s, 4s, 8s…),
capped at some maximum, and *randomize* each delay so clients spread out. The canonical formula:

```
delay = min(cap, base * 2**attempt) * (1 + uniform(-jitter, +jitter))
```

**The implementation.** `retry.py` exposes the logic in two forms:

- `retry_call(fn, *args, on=(SomeError,), attempts=5, base, cap, jitter, sleeper, rng, **kwargs)` —
  the functional form. It calls `fn`; if `fn` raises one of the exception types in `on`, it computes
  a delay, sleeps, and tries again, up to `attempts` times. After the final attempt it **re-raises
  the original exception** (so callers still catch the meaningful `SftpUploadError`, not a generic
  wrapper). An optional `wrap_final=True` instead raises `RetryExhausted` carrying the attempt count.
- `@retry(...)` — the decorator form, for wrapping a whole function.

Two design choices make this testable and honest:

1. **The clock is injectable.** `sleeper: Callable[[float], None] = time.sleep`. In production it is
   the real `time.sleep`; in tests you pass `lambda _: None` and the suite spends *zero* real time
   while still exercising the full retry path. Likewise `rng: random.Random` is injectable so a test
   can pin the jitter and assert exact delays.
2. **It retries only the exceptions you name.** `on=(SftpUploadError,)` means a programming bug
   (say, a `TypeError`) is *not* retried — it propagates immediately. Retrying bugs just wastes time
   and hides them.

`_compute_delay` is a pure function — `(attempt, base, cap, jitter, rng) -> float` — which makes the
backoff math independently testable without any I/O at all.

> **A subtlety: not everything retryable is an exception.** The count validator (Part VI) needs to
> retry on a *value comparison* ("does ES's count equal my row count yet?"), not on an exception. So
> it deliberately hand-rolls the same backoff loop instead of using `retry_call`. This is a
> reasonable seam: `retry_call` is for "retry on exception"; a value-based retry is a different shape
> and forcing it through the exception machinery would be awkward. Recognizing when an abstraction
> does *not* fit is as important as building it.

---

# Part III — The control plane (Kafka)

## 10. Why Kafka triggers the work but does not carry it

We established the control/data split in §2. Here is the consumer-side theory that follows from it.

A Kafka topic is a **durable, partitioned, append-only log**. Each partition is an ordered sequence
of messages, each with a monotonic **offset**. A consumer reads forward through a partition and
periodically records (*commits*) how far it has read. The committed offset is the consumer's
bookmark: on restart or rebalance, it resumes from there.

Because the control topic carries only tiny trigger messages, the consumer is simple: read one
message, do the (potentially long) job it points to, then advance the bookmark. We are not trying to
maximize throughput — we are trying to process each trigger *reliably and exactly in order*. That
priority shapes every choice below.

## 11. Delivery semantics: at-least-once and manual offset commit

Every messaging system forces a choice among three delivery guarantees:

- **At-most-once:** commit the offset *before* doing the work. If the work fails, the message is
  already acknowledged and is lost. You never do work twice, but you can drop work. Unacceptable
  here — we must not silently skip an export.
- **At-least-once:** commit the offset *after* the work succeeds. If the work fails, the offset was
  not advanced, so the message is redelivered. You never drop work, but you might do it twice.
  **This is what we want**, paired with idempotent jobs so that "twice" is harmless.
- **Exactly-once:** achievable only with transactional coordination across the consumer and the
  sink. Far more machinery than this problem warrants.

So the rule is: **disable auto-commit, and commit manually only after the entire job succeeds.**
That is why `KafkaConfig.confluent_config()` hard-codes `"enable.auto.commit": "false"`. Auto-commit
would advance the bookmark on a timer regardless of whether the job finished — silently converting us
to a lossy at-most-once system. We forbid it at the source.

There is a second config worth understanding: `"auto.offset.reset": "earliest"`. This decides where
a *brand-new* consumer group starts when it has no committed offset yet — from the beginning of the
topic (`earliest`) or only new messages (`latest`). `earliest` is the safe default for a job runner:
a freshly deployed service picks up any pending work rather than ignoring it.

## 12. The `ControlConsumer`

`control_consumer.py` is a thin wrapper around `confluent_kafka.Consumer`. "Thin" is intentional —
it adds exactly three things and no more: a decode step, poison-message handling, and a manual-commit
callback. Its constructor signature is the key to its testability:

```python
class ControlConsumer:
    def __init__(self, cfg: KafkaConfig, *,
                 consumer_factory: Callable[[dict[str, str]], Any] | None = None) -> None:
```

**The injected factory is the seam.** In production, `consumer_factory` is `None`, so the class
imports and uses the real `confluent_kafka.Consumer`. In tests, you pass a `FakeConsumer` factory and
never touch a real broker. This is ports-and-adapters in miniature: the *port* is "something that can
be constructed from a config dict and can `poll`/`commit`/`subscribe`/`close`"; the real Kafka client
and the fake are two *adapters*. (Note the local `import confluent_kafka` inside the constructor: it
keeps the heavy native dependency from being imported until it is actually needed, which keeps the
pure modules importable in any environment.)

**Decoding** is isolated in a static method so it can be tested directly:

```python
@staticmethod
def _decode(raw: bytes, *, partition: int, offset: int) -> ControlMessage: ...
```

It parses JSON, requires the payload to be an object with a non-empty `job_doc_id` (accepting `id`
as an alias), validates `correlation_id`'s type, and returns a frozen `ControlMessage`. Anything
malformed raises `ControlMessageError`. Crucially, the partition and offset are captured *into* the
`ControlMessage` so that later we can commit *this exact message's* offset.

**`iter_messages` is a generator** that yields `(ControlMessage, commit_fn, raw_msg)` tuples:

```python
def iter_messages(self, *, poll_timeout_s: float = 1.0,
                  stop: Callable[[], bool] | None = None
                  ) -> Iterator[tuple[ControlMessage, Callable[[], None], Any]]:
```

Three details deserve explanation:

1. **The commit is a closure passed back to the caller.** The consumer does *not* commit on its own;
   it hands the orchestrator a `commit_fn` to call *after* success. This keeps the "when do we
   acknowledge?" decision in the orchestrator, where the success/failure of the whole job is known.
   The closure captures the message via a default argument (`def _commit(_m=msg): ...`) — a small but
   important trick that avoids the classic Python late-binding bug where a loop closure captures the
   *variable* rather than its *current value*.

2. **Poison messages are skipped by committing past them.** If `_decode` raises
   `ControlMessageError`, the message can *never* succeed no matter how many times we retry it —
   it is structurally broken. Retrying it forever would wedge the consumer ("poison pill"). So we log
   it loudly and commit past it. (A null-valued record — a Kafka tombstone — is handled the same way.)
   This is a deliberate distinction: *malformed control messages* are skipped; *valid jobs that fail*
   are not, as we will see next.

3. **The `stop` callback enables graceful shutdown.** The loop checks `stop()` each iteration; the
   daemon flips it on `SIGTERM` so the process finishes the current job and exits cleanly.

## 13. The daemon loop and process lifecycle

`__main__.py` is the conductor. `main()` does the following:

1. **Load settings; exit 2 on `ConfigError`.** Logging is not configured yet, so it prints to stderr
   and returns a distinct exit code. A bad config is operator error, separable from a runtime crash.
2. **Configure logging.**
3. **Install signal handlers.** `SIGINT`/`SIGTERM` set a `stopping` flag. We do not abort
   mid-job — we let the current job finish and then the loop's `stop()` check ends it. This is
   graceful shutdown: no half-written, half-uploaded job.
4. **Build the Elasticsearch client** (`_build_es_client`), wiring API-key or basic-auth from config.
5. **Run the loop:** for each `(ctrl, commit, _raw)` from the consumer, call `run_one(...)`; on
   success, `commit()`; on `EtlError`, handle the failure.

The loop is where at-least-once becomes real, and where a **genuinely subtle correctness bug** lives
if you are not careful. Here is the corrected logic:

```python
for ctrl, commit, _raw in consumer.iter_messages(stop=lambda: stopping["flag"]):
    try:
        run_one(ctrl=ctrl, es=es, settings=settings)
    except EtlError as e:
        _log.error("job failed; halting without commit so it is redelivered: %r", e, extra=...)
        exit_code = 1
        break          # <-- halt, do NOT continue
    commit()
    _log.info("job committed", extra=...)
```

**Why `break` and not `continue`?** This is the heart of it. Kafka offset commits are *cumulative*:
committing the offset of message *N* means "I have processed everything up to and including *N*."
Now imagine message 5 fails and we `continue` (without committing), then message 6 succeeds and we
commit 6. Committing 6 marks 5 as done too — **we just silently lost the failed job.** The "it will
be redelivered on restart" guarantee is quietly violated by the very next success.

The fix is to **halt on a hard failure**: stop the loop without committing, and exit non-zero. The
offset stays put, the orchestrator restarts us, and message 5 is redelivered. We fail loudly and lose
nothing. (By the time an `EtlError` reaches this boundary, the pipeline has already exhausted its
*internal* retries — five count re-queries, a full re-extract, five SFTP attempts — so a failure here
is a genuine, persistent problem worth halting and alerting on.)

This is the kind of bug that passes every test, works in every demo, and corrupts data in production
once a month. The lesson: **understand your infrastructure's exact semantics** ("commits are
cumulative") before you rely on them.

> **Production note.** Halting is the conservative MVP-correct choice. A more sophisticated service
> might instead `seek` back to the failed offset and retry in-process with backoff (keeping the
> daemon alive), or route the failed job to a dead-letter topic for later inspection. Those are real
> tickets (DLQ + alerting) for the "full product" stage — but they are *enhancements* of a correct
> baseline, not fixes for a broken one.

---

# Part IV — The data source (Elasticsearch)

## 14. Data-driven jobs: the job document pattern

Rather than hard-coding "the daily sales export queries index X with filter Y and writes columns Z,"
we store that description **as data** — a document in an Elasticsearch index. The control message
carries only the document's ID. This is the **data-driven / interpreter pattern**: behavior is
expressed as data that the engine interprets, not as code that must be redeployed.

A job document looks like this:

```json
{
  "job_id": "daily-sales-export",
  "data_index": "sales-2026-05",
  "query": { "range": { "ts": { "gte": "now-1d" } } },
  "column_paths": {
    "order_id": "order_id",
    "customer": "customer.name",
    "amount":   "totals.amount_cents",
    "first_sku":"items[0].sku"
  },
  "columns": ["order_id", "customer", "amount", "first_sku"],
  "remote_filename": "upload/daily-sales-2026-05-26.csv"
}
```

The payoff: **adding or changing an export requires no code change and no deploy.** You write a new
job document and emit a control message referencing it. The engine stays generic. The trade-off —
which we address in the next section — is that data can be malformed in ways code cannot, so the
engine must validate the document defensively.

This is modeled in `models.py` as a frozen `JobSpec` dataclass: `job_id`, `data_index`, `query`
(the ES query DSL body), `column_paths` (the column-to-path map), `columns` (ordered output
columns), and `remote_filename`. Plain dataclasses, not pydantic — the validation we need is simple
and explicit, and avoiding a heavy dependency keeps the surface small.

## 15. The job loader: validation at the boundary

`job_loader.py` has one function:

```python
def load_job(es: Any, *, job_index: str, job_doc_id: str) -> JobSpec: ...
```

It performs a `GET` by ID and then validates *every* field before constructing a `JobSpec`:

- The ES call is wrapped so any client error becomes `ElasticsearchQueryError` (a 404 surfaces as
  the client's `NotFoundError`, which we wrap).
- Required fields (`data_index`, `query`, `columns`, `remote_filename`) must be present, else
  `JobSpecError`.
- `query` must be an object; `columns` must be a `list[str]`; `column_paths` (optional) must be a
  `dict[str, str]`; `remote_filename` must be a non-empty string.

**The principle is "validate at the boundary."** The moment data crosses from the outside world
(here, an ES document an operator wrote) into the typed core of the program, it gets checked. After
`load_job` returns, the rest of the pipeline can treat the `JobSpec` as trustworthy and not
re-litigate "is `columns` really a list?" at every step. All the suspicion is concentrated in one
place. A malformed document fails *here*, with a precise message, before any extraction begins.

## 16. Deep pagination theory: why `from`/`size` breaks

This section is pure theory, because it justifies the two implementations that follow. To export a
large result set you must read it in pages. There are three ways, and the differences matter.

**Naive `from`/`size` (offset pagination) — and why it fails at depth.** The obvious approach is "give
me results 0–999, then 1000–1999, …" via `from` and `size`. This breaks for deep exports for two
reasons:

1. **Cost.** To return results 1,000,000–1,000,999, Elasticsearch must internally find, sort, and
   discard the first 1,000,000 on *every shard*. Cost grows with depth. ES enforces an
   `index.max_result_window` (default 10,000) precisely to stop you from doing this. Deep offset
   pagination is quadratic-ish work for linear data.
2. **Correctness under concurrent writes.** Offsets are positions in a *live* result set. If
   documents are inserted or deleted while you page, the window shifts under you — you can skip or
   double-count rows. For an export that must match a count exactly, that is fatal.

The fix for both is to paginate over a **consistent point-in-time view** using a **stable cursor**
rather than a numeric offset. Two cursor mechanisms exist.

**Scroll API.** The classic one: you open a server-side *scroll context*, get a `_scroll_id`, and call
`scroll` repeatedly until the pages run dry, then `clear_scroll`. It is what the original NiFi flow
used. Its downside is that the scroll context holds search resources on the cluster for its whole TTL
and is *stateful server-side*, which makes it awkward for load-balanced or long-lived use. Elastic now
recommends against it for new code.

**Point-In-Time (PIT) + `search_after`.** The modern replacement, and the one this project uses. You
open a **PIT** — a lightweight, named consistent view — then issue *stateless* searches that sort by a
stable key and use `search_after` to say "give me what comes after this sort value." Each request is
independent (easy to balance and reason about), and sorting on `_shard_doc` (a cheap, stable,
per-shard tie-breaker available only inside a PIT) makes the cursor both stable and efficient. You
close the PIT when done.

**The decision in this project:** use PIT + `search_after` exclusively. An earlier iteration kept a
pluggable Scroll strategy for one-to-one NiFi parity, but carrying two code paths — and the
Strategy-pattern factory to switch between them — earned its keep only while Scroll was a live option.
With PIT the recommended mechanism and no requirement to mimic Scroll's exact semantics, we collapsed
to the single strategy the next three sections describe.

## 17. Point-in-time + `search_after`: a stable cursor

The whole mechanism rests on one idea: **page with a stable cursor over a frozen view**, never with a
numeric offset. Three pieces make that work.

**The point-in-time (PIT).** `open_point_in_time(index, keep_alive)` returns a PIT id naming a
consistent snapshot of the index. Every search that passes that id sees the same set of documents,
even if writes land while you page — which is exactly the property §16 said an exact-count export
needs. `keep_alive` (e.g. `"5m"`) is how long ES retains the snapshot between requests; each request
renews it.

**The sort key.** Inside a PIT you may sort on `_shard_doc` — a synthetic, monotonic, per-shard
tie-breaker that is both cheap and *total* (no ties). A total order is what makes a cursor
unambiguous: there is always a well-defined "next" document.

**The cursor.** Each response's last hit carries a `sort` array (its `_shard_doc` value). Feed that
back as the next request's `search_after`, and ES returns "everything after that point." Repeat until
a page comes back empty. The cursor is a *value in the data*, not a position in a result set, so it
cannot drift the way `from`/`size` does.

We also set `track_total_hits: False`: the validator computes the authoritative count separately via
`_count` (§24), so paying for an exact total on every page would be wasted work.

## 18. The pagination generator: streaming + lifecycle

`es_extract/pagination.py` implements the strategy as a single **generator**, `SearchAfterPagination`:

```python
@dataclass
class SearchAfterPagination:
    keep_alive: str = "5m"
    source_only: bool = True
    error_cls: type[Exception] = EsExtractError
    def iter_hits(self, *, es, index, query, page_size) -> Iterator[dict]:
        try:
            pit = es.open_point_in_time(index=index, keep_alive=self.keep_alive)
        except Exception as e:
            raise self.error_cls(f"open_point_in_time failed: {e!r}") from e
        pit_id = pit.get("id")
        try:
            search_after = None
            while True:
                body = {"size": page_size, "query": query,
                        "pit": {"id": pit_id, "keep_alive": self.keep_alive},
                        "sort": [{"_shard_doc": "asc"}], "track_total_hits": False}
                if search_after is not None:
                    body["search_after"] = search_after
                resp = es.search(body=body)
                pit_id = resp.get("pit_id", pit_id)
                hits = resp["hits"]["hits"]
                if not hits: break
                for h in hits:
                    yield h.get("_source", {})        # whole hit if source_only=False
                search_after = hits[-1].get("sort")   # cursor = last row's sort values
                if not search_after: break
        finally:
            if pit_id is not None:
                try: es.close_point_in_time(body={"id": pit_id})
                except Exception as e: _log.warning("close_point_in_time failed: %r", e)
```

Three design points, each a transferable lesson:

1. **Generators give us streaming for free.** `iter_hits` `yield`s one `_source` at a time. The
   caller pulls hits lazily; we never materialize the whole result set. The page size bounds memory.
   The same code handles five rows or five million.

2. **Resource lifecycle lives in `finally`.** A PIT is a server-side resource that *must* be released.
   The `try/finally` guarantees `close_point_in_time` runs **even if the consumer abandons iteration
   partway** — for example, if a downstream stage raises and the generator is closed early. This is
   the critical correctness property of a resource-owning generator: the cleanup is bound to the
   generator's lifetime, not to "reaching the last line." Forgetting it leaks PITs until their
   keep-alive reaps them, holding search resources on the cluster.

3. **Cleanup failures are swallowed (logged, not raised).** If `close_point_in_time` itself fails, we
   log and move on. Why? Because the keep-alive will reap the PIT anyway, and raising from cleanup
   would *mask the original exception* that triggered the cleanup. Cleanup should never overwrite the
   real error.

Errors from the ES calls are wrapped in `error_cls` — `EsExtractError` by default, or whatever the
host injects (the `etl` daemon injects `ElasticsearchQueryError`, keeping the "all ES failures look
the same to the daemon" contract from §7).

## 19. `es_extract`: a standalone, reusable extraction package

The pagination generator, the `_count` helper, and a streaming diagnostic do not live under `etl` at
all — they live in `src/es_extract/`, a package that depends on **only** the `elasticsearch` client
and the standard library, with no import of anything application-specific. You can copy the directory
out, or `pip install` this repo, and use it anywhere:

```python
from elasticsearch import Elasticsearch
from es_extract import count, iter_hits

es = Elasticsearch("http://localhost:9200")
print(count(es, "my-index", {"match_all": {}}))
for src in iter_hits(es, "my-index", {"match_all": {}}):   # PIT + search_after, streamed
    ...
```

Two seams keep it reusable without forcing one host's choices on every caller:

* **`source_only`.** By default each yielded value is the hit's `_source` dict — the common case. Pass
  `source_only=False` to receive the whole envelope (`_id`, `_score`, `sort`), for callers that need
  document ids or relevance.
* **`error_cls`.** Every ES failure is wrapped in an injectable exception type, defaulting to
  `EsExtractError`. A host application passes its own — `etl` passes `ElasticsearchQueryError` — so
  failures land in the host's existing `except` boundary while the package itself stays ignorant of
  that hierarchy. This is dependency injection applied to *error taxonomy*.

The package also ships `tee_to_ndjson`, a generator that wraps a hit stream and writes each hit to an
NDJSON file *as it passes through*, without buffering — the pipeline uses it (opt-in, via
`ES_RAW_DUMP_DIR`) to capture exactly what came out of ES for diagnostics. Because `es_extract` has no
`etl` dependency, you can exercise it in isolation against a real cluster with
`scripts/try_es_extract.py` (it can even seed a throwaway index first): it builds a `JobSpec`, runs
`count` + `iter_hits`, and checks the streamed total against `_count`.

## 20. The extractor: a thin, honest seam

`extractor.py` is intentionally tiny. It owns two responsibilities and delegates the rest:

```python
def expected_count(es, index, query) -> int:
    resp = es.count(index=index, body={"query": query})
    return int(resp.get("count", 0))

def iter_hits(es, job, *, page_size, keep_alive="5m") -> Iterator[dict]:
    return es_extract.iter_hits(es, job.data_index, job.query, page_size=page_size,
                                keep_alive=keep_alive, error_cls=ElasticsearchQueryError)
```

`expected_count` issues the `_count` query — the *ground truth* the validator will check against.
`iter_hits` is a thin adapter that unpacks a `JobSpec` and hands the pieces to `es_extract`, pinning
failures to `ElasticsearchQueryError`.
Keeping this seam thin matters: the extractor is the one place that knows "a job has a count and a
stream of hits," and it expresses that without entangling itself in *how* the hits are produced.

> **The extraction layer is a standalone package.** As §19 details, the pagination generator, the
> `_count` helper, and the NDJSON diagnostic live in `src/es_extract/` with no dependency on `etl`.
> `extractor.py` is the one wrapper that injects `etl`'s own `ElasticsearchQueryError` (via
> `error_cls`) so failures still land in the daemon's single `EtlError` boundary (§7). This is
> ports-and-adapters taken one step further: the data-source adapter is independently reusable, while
> the application keeps its own error taxonomy.

> **A forward-compatibility note.** These calls pass `body={"query": query}`. In elasticsearch-py
> 8.x this is *deprecated* (it works but warns) and is *removed* in 9.x — which is exactly why the
> dependency is pinned `>=8,<9`. Migrating to explicit keyword arguments (`es.count(index=..., query=...)`,
> `es.search(index=..., query=..., size=..., sort=..., pit=...)`) is a clean, well-scoped "production
> hardening" ticket. It is called out here because *knowing where your deprecations are* is part of
> understanding the system.

---

# Part V — Transformation & output

## 21. From documents to rows: the projection problem

Elasticsearch returns rich, nested JSON documents. A CSV is a flat grid. The transform step bridges
the two: it **projects** each nested document down to a flat row of named columns.

The original design called for **JOLT** — the JSON-to-JSON transformation DSL used by NiFi's
`JoltTransformJSON`. JOLT is powerful (shifts, defaults, cardinality changes, modifications). But
investigation found **no maintained Python port of JOLT on PyPI** (the package some references name,
`pyjolt-transform`, does not exist). Rather than take on a heavy or unmaintained dependency — or stand
up a JVM sidecar just to run JOLT — we asked: *what does this flow actually need?*

The honest answer: **flat extraction of named fields from nested documents.** Every export shape we
have is "take `customer.name` and call it `customer`; take `items[0].sku` and call it `first_sku`."
That is not a transformation *language*; it is a **path projection**. So we built exactly that and no
more. This is a recurring engineering virtue: **build the abstraction the problem needs, not the most
general one imaginable.** A dotted-path projector is a few dozen lines, fully testable, with zero
dependencies; a JOLT engine is a maintenance burden we would use 5% of.

## 22. The transformer: dotted-path projection

`transformer.py` defines a tiny path language and a projector. The path syntax:

- `a` — a top-level key
- `a.b.c` — nested object keys
- `a.b[0]` — a list index (`[N]` only; no slices or wildcards)
- `a.b[0].c` — any mix

The core resolver is a pure function:

```python
def get_by_path(doc: Any, path: str) -> Any:
    """Resolve a dotted path inside `doc`; return "" when any step is missing."""
```

It tokenizes the path (a small regex splits `items[0].sku` into `["items", "[0]", "sku"]`) and walks
the document one step at a time. At each step it checks the *type* matches the token: an `[N]` token
requires a list with that index in range; a name token requires a dict containing that key. **Any
miss — wrong type, missing key, out-of-range index, or a `None` encountered mid-walk — returns the
empty string `""`.** This "missing becomes empty" rule is what makes exports robust to real-world
ragged data: a document missing `items` does not crash the job; that cell is just blank.

Two higher-level functions build on it:

```python
def project(hit, columns, column_paths, *, job_id, hit_id=None) -> dict:
    # for each column, look up its path (defaulting to the column name itself), resolve it
def iter_transformed(hits, column_paths, columns, *, job_id) -> Iterator[dict]:
    # apply `project` lazily across the whole hit stream
```

`project` has a useful default: if a column has no entry in `column_paths`, its path *is its own
name* — so a column called `order_id` maps to the top-level `order_id` field automatically. You only
list the columns whose source path differs from their output name. The one thing that *does* raise
(`TransformError`, carrying `job_id` and `hit_id`) is a *configured-but-empty* path — that is an
operator mistake in the job document, not ragged data, and should fail loudly.

`iter_transformed` is again a **generator**, so transformation joins the same lazy stream as
extraction: a hit flows from ES → through projection → toward the CSV writer without the full set ever
being in memory.

> **MVP-to-product seam.** If a future job genuinely needs richer logic — derived columns,
> conditionals, value mapping — the right move is a small explicit pre-step *in this module*, not a
> resurrected JOLT dependency. The module docstring says exactly this, so the next engineer is guided
> toward the intended extension point.

## 23. Streaming CSV and the integrity sidecar

`csv_writer.py` writes the flat rows to disk and, in the *same pass*, computes a SHA256 checksum. The
public function:

```python
def write_csv(rows: Iterable[dict], columns: list[str], csv_path: Path) -> CsvResult: ...
```

Two design ideas carry this module.

**(a) Single-pass hashing via a tee'ing wrapper.** A naive implementation writes the file, then reads
it back to hash it — two full passes over the data. Instead we wrap the file object:

```python
class _HashingWriter:
    def __init__(self, fh, hasher):
        self._fh, self._hasher = fh, hasher
    def write(self, s: str) -> int:
        b = s.encode("utf-8")
        self._fh.write(b)        # to disk
        self._hasher.update(b)   # and into the running SHA256
        return len(s)
```

`csv.DictWriter` writes through this wrapper, so every byte that hits the disk *simultaneously*
updates the hash. The file is hashed exactly once, incrementally, regardless of size — consistent
with the streaming philosophy. This is the **decorator pattern** at the I/O level: we wrap a
file-like object to add a behavior (hashing) transparently to the writer using it.

**(b) The checksum sidecar.** Alongside `file.csv` we write `file.csv.sha256` in the standard
`sha256sum` format — `"<hex>  <filename>\n"`. The recipient can run `sha256sum -c file.csv.sha256`
to verify the file arrived intact. This is **end-to-end integrity**: the check is computed at the
source and verified at the destination, so any corruption *anywhere in between* (disk, transfer,
storage) is caught. It is cheap insurance that turns "the file looks truncated" support tickets into
a one-command diagnosis.

Other details: `extrasaction="ignore"` means stray keys in a row dict are dropped rather than
crashing; `lineterminator="\n"` forces consistent line endings so the hash is platform-independent;
a `_stringify` helper renders `None` as empty and `bool` as `"true"`/`"false"` deterministically; and
the header is always written, so even a zero-row export produces a valid, well-formed CSV. The result
is returned as a frozen `CsvResult(csv_path, sidecar_path, row_count, sha256_hex)` — the `row_count`
is what the validator will scrutinize next.

---

# Part VI — Trust & delivery

## 24. Validation theory: why counts disagree, and the two-tier strategy

This is the component that justifies the whole rewrite, so it earns a careful explanation.

**The check.** We compare two numbers: the count ES reports for the query (`_count`) and the number
of rows we actually wrote to the CSV. If they differ, we may have silently dropped (or duplicated)
data, which is the worst possible outcome for an export — *worse than failing*, because a wrong file
looks right.

**Why they might legitimately disagree, transiently.** A subtlety of Elasticsearch is that it is
**near-real-time, not strictly consistent.** Newly indexed documents become searchable only after a
*refresh* (by default ~1 second). And `_count` and a paginated read are *separate operations* that
can observe the index at slightly different moments. So a momentary mismatch can be a *refresh race*,
not real data loss — the two reads simply caught the index mid-refresh. If we failed on the first
mismatch, we would raise false alarms on a perfectly correct export.

**The two-tier strategy** (`validator.py`, `validate_with_retry`) balances "don't cry wolf" against
"don't ship a wrong file":

- **Tier 1 — re-query the count, up to N times (default 5), with exponential backoff + jitter.** Each
  retry re-issues `_count` and compares to the row count. This gives transient refresh races time to
  settle. A match at any point returns success immediately. (As noted in §9, this loop is hand-rolled
  rather than using `retry_call`, because it retries on a *value comparison*, not on an exception.)
- **Tier 2 — one full re-extract.** If the count still disagrees after all Tier-1 retries, the problem
  might be on *our* side (a hiccup during the original extraction). So we invoke a caller-supplied
  callback that re-runs the entire extract→transform→CSV step, then compare against a fresh `_count`.
- **Fail.** If it *still* mismatches, we raise `RecordCountMismatch` carrying the full attempt history
  (`[(es_count, csv_rows), ...]`). The job aborts; the offset is not committed; the file is not
  shipped. We fail loudly, with evidence.

**The callback (inversion of control).** The validator does not know *how* to re-extract — that would
couple it to the extractor, the strategy, and the CSV writer. Instead it accepts
`on_full_reextract: Callable[[], CsvResult]` and *calls back* into the orchestrator, which knows how
to rebuild the whole step. This is **inversion of control**: the low-level policy (validation)
delegates the high-level action (re-extraction) to its caller. It keeps the validator focused and
independently testable — a test passes a fake callback and asserts it was called exactly once.

The signature reflects all of this, including the injectable clock/RNG for fast, deterministic tests:

```python
def validate_with_retry(*, es, index, query, csv_result: CsvResult, retry_cfg: RetryConfig,
                        on_full_reextract: Callable[[], CsvResult],
                        sleeper=None, rng=None, log_extra=None) -> CsvResult: ...
```

It returns the `CsvResult` that *ultimately* matched (which may be the re-extracted one), so the
orchestrator uploads the validated file.

## 25. SFTP delivery: subprocess over library, and the security posture

The final step ships the CSV and its sidecar to the partner over SFTP. Two decisions define this
module, and both are about *trust*.

**Decision 1: shell out to the `sftp` binary instead of using a library (paramiko).** This is
deliberate. The system `sftp` client (OpenSSH) is ubiquitous, battle-tested, audited, and maintained
by people whose full-time job is SSH security. A pure-Python SSH library is one more dependency to
keep patched and one more attack surface. By invoking the OS client we inherit its hardening and its
`known_hosts` handling for free. The cost — building a batch file and parsing exit codes — is small
and contained.

**Decision 2: enforce strict host-key checking, always.** This is the security crux of the whole
service, so it is worth being explicit about the threat. SFTP runs over SSH. The first time you
connect to a host, SSH learns its public host key. On every subsequent connection it verifies the
host still presents that key — this is what stops a **man-in-the-middle**: an attacker who intercepts
your connection cannot present the real host's key, so the connection is refused. The dangerous
"convenience" setting `StrictHostKeyChecking=no` *disables* that verification and will happily upload
your data to whoever answers — the single most common SFTP security mistake. **This service never uses
it.** It pins `StrictHostKeyChecking=yes` against an operator-supplied `known_hosts` file.

`sftp_uploader.py` builds this up carefully:

- `UploadPlan(local: Path, remote: str)` pairs each local file with its remote destination.
- `_build_batch(plans)` emits an `sftp` batch file (`put <local> <remote>` lines, then `bye`), using
  `shlex.quote` on paths to handle spaces safely.
- `_run_sftp(cfg, batch_text, *, timeout)` writes the batch to a temp file and invokes:

  ```python
  argv = ["sftp", "-b", batch_file, "-i", key_path, "-P", port,
          "-o", f"UserKnownHostsFile={known_hosts}",
          "-o", "StrictHostKeyChecking=yes",
          "-o", "BatchMode=yes",
          f"{user}@{host}"]
  subprocess.run(argv, check=False, capture_output=True, timeout=timeout)
  ```

Every element of that command is a security choice:

- **`argv` is a list, not a string, and there is no `shell=True`.** This is immune to shell
  injection: even if a filename contained `; rm -rf /`, it is passed as one literal argument, never
  interpreted by a shell. Building shell command *strings* from external input is one of the oldest
  and deadliest bug classes; using a list argv sidesteps it entirely.
- **`BatchMode=yes`** disables any interactive prompt. Without it, an unexpected password prompt would
  hang the process forever; with it, the operation fails fast instead.
- **`-i key_path` (key auth)** — no passwords in the process or environment.
- **`timeout`** bounds the whole operation so a stalled network cannot wedge the daemon.
- Non-zero exit, a timeout, or a missing `sftp` binary are each turned into `SftpUploadError` (with
  stderr captured for diagnosis).

Finally, `upload(...)` wraps `_run_sftp` in `retry_call(on=(SftpUploadError,), ...)` so transient
network failures get the same five-attempt exponential-backoff treatment as everything else. A
genuinely unreachable host fails after five tries, the error propagates, and — per §13 — the daemon
halts without committing, so the job is redelivered later.

Note what is *not* logged: the logged `argv` contains the key *path*, never key material, and SASL/ES
passwords never appear in logs. Secret hygiene is a property you maintain deliberately.

---

# Part VII — Putting it together

## 26. The orchestrator: `run_one`

`pipeline.py` contains the conductor for a single job:

```python
def run_one(*, ctrl: ControlMessage, es: Any, settings: Settings) -> None: ...
```

Read it as a narrative and you have the whole pipeline in one screen:

1. Build a `log_extra` dict from the control message (job id, correlation id, partition, offset) so
   every log line for this job is tagged.
2. `load_job(...)` → `JobSpec`. Enrich `log_extra` with `job_id` and `data_index`.
3. `expected_count(...)` → record N up front (and log it).
4. The pipeline reads `settings.pagination.pit_keep_alive` for the upcoming PIT + `search_after` pass.
5. `_do_extract_to_csv(...)` chains the three lazy stages —
   `iter_hits → iter_transformed → write_csv` — into a `CsvResult`.
6. `validate_with_retry(..., on_full_reextract=_reextract)`. The `_reextract` closure re-runs
   `_do_extract_to_csv` — a fresh call opens a new point-in-time (the previous one is spent) —
   overwriting the same local path, idempotent by construction.
7. `upload(settings.sftp, [UploadPlan(csv), UploadPlan(sidecar)], retry_cfg=...)`.

The function deliberately **does not commit the Kafka offset** — that decision belongs to `__main__`,
which alone knows whether the *whole* loop iteration (including the commit itself) succeeded. This is
separation of concerns: `run_one` knows how to *do* a job; `__main__` knows what to do about *success
or failure* of a job.

Notice the **dependency injection** in the signature: `es` and `settings` are passed in, not imported
as globals. That is precisely why `test_pipeline.py` can drive `run_one` end-to-end with a fake ES and
a temp directory, asserting the CSV is produced and `subprocess.run` is called — with no Kafka, no ES,
no SFTP server anywhere in sight.

## 27. A full trace of one control message

Tying every part together, here is the life of a single message, success path:

1. A producer emits `{"job_doc_id": "daily-sales-export"}` to the control topic.
2. `ControlConsumer.iter_messages` polls, `_decode` validates it into a `ControlMessage(partition=0,
   offset=42)`, and yields `(ctrl, commit_fn, raw)`.
3. `__main__` calls `run_one(ctrl, es, settings)`.
4. `load_job` GETs the `daily-sales-export` document and validates it into a `JobSpec`.
5. `expected_count` runs `_count` → say **5**.
6. The extractor prepares a PIT + `search_after` pass over `data_index` (the stable-cursor pagination).
7. `iter_hits` streams 5 `_source` dicts; `iter_transformed` projects each via `column_paths`;
   `write_csv` streams them to `/tmp/etl-csv/daily-sales-2026-05-26.csv`, hashing as it goes, and
   writes the `.sha256` sidecar. `CsvResult.row_count == 5`.
8. `validate_with_retry` re-queries `_count` (still 5) — matches on the first try, returns.
9. `upload` builds a batch (`put` csv, `put` sidecar, `bye`) and runs `sftp` with strict host-key
   checking; exit 0.
10. `run_one` returns. `__main__` calls `commit_fn()`, advancing the offset past 42. The message is
    now durably acknowledged.

If step 5–9 had raised an `EtlError` (count mismatch after all retries, or SFTP unreachable after all
retries), step 10 would *not* happen; the daemon would log, halt, and exit non-zero, and on restart
the same message (offset 42, still uncommitted) would be redelivered — and because the job is
idempotent, re-running it is safe.

---

# Part VIII — Quality & evolution

## 28. Testing strategy: seams, fakes, and fixtures

The whole architecture was built to make this section easy. **The unit test suite needs no network
and no Docker, and runs in well under a second.** That is not luck; it is the payoff of every
dependency-injection seam described above.

**Fakes over mocks.** Where practical the tests use small hand-written *fakes* (e.g. a `FakeConsumer`
with `poll`/`commit`/`subscribe`, a `FakeMessage`) rather than `MagicMock`. A fake encodes the real
contract ("`poll` returns a message or `None`") in readable code, so a test reads like a story and
breaks loudly if the contract changes. Mocks are reserved for boundaries we do not own and want to
assert *calls* against — notably `subprocess.run`, which the SFTP tests monkeypatch to assert the
exact `argv` (proving `StrictHostKeyChecking=yes` is present) without ever opening a socket.

**The seams that make this possible**, recapped: the injectable `consumer_factory`; the `es` and
`settings` parameters threaded everywhere instead of globals; the injectable `sleeper` and `rng` in
the retry/validator code (so a test exercises five "retries" in zero real time and with deterministic
jitter); and the `on_full_reextract` callback (so the validator's Tier-2 path is testable with a fake
callback). Each seam exists because somewhere a test needed to substitute reality.

**Coverage of behavior, not just lines.** The suite asserts the things that *matter*: the golden path
produces the right CSV and commits; a count mismatch through all retries plus re-extract raises and
does **not** commit; an SFTP failure retries five times then raises; a transient SFTP failure that
recovers on the third attempt commits; a poison message is skipped by committing past it; resources
(the PIT) is released even when iteration is abandoned. These are the *requirements* expressed as
executable checks — which is what makes the system safe to change later. The current state: 72 tests
passing, `ruff` and `mypy --strict` clean, ~88% line coverage.

**Hermeticity is a feature, not an accident.** Recall the `.env` bug from §6: a test that depends on
the absence of a file on disk is not hermetic. The autouse fixture that neutralizes `load_dotenv`
restored hermeticity. The general rule: a unit test must depend only on its explicit inputs.

## 29. The local mock environment

For end-to-end smoke testing, `docker-compose.yml` brings up a stack that mirrors the *shapes* of
production without its weight:

- **Kafka** in KRaft mode (no ZooKeeper) — a single-broker control plane.
- **Elasticsearch** single-node with security disabled — the data plane.
- **`atmoz/sftp`** — a key-only SFTP server, the delivery target.
- Optional **Kibana** and **Kafka-UI** behind a `ui` profile, for eyeballing indices and topics.

The supporting scripts (`scripts/`) make it one command: `setup_local.sh` boots the stack, generates
the SSH client key *and* the server host keys, captures the host key into `known_hosts` (so the
strict host-key checking from §25 has something to check against), and creates the control topic;
`seed.py` writes a job document, indexes a deterministic set of sample documents, and produces one
control message; `teardown_local.sh` tears it down (with a `--purge` to wipe volumes).

The value of this stack is that it lets you exercise the *real* integration code — the actual
`confluent_kafka.Consumer`, the actual `elasticsearch` client, the actual `sftp` binary — against
disposable infrastructure. It is where the failure-mode drills live: tamper with ES so the count
disagrees and watch the two-tier validator do its thing; stop the SFTP container and watch the five
retries then the clean halt; kill the daemon mid-job and watch redelivery on restart. These drills
are how you build *confidence* that the theory in Parts III–VI actually holds.

## 30. From MVP to full product: an incremental roadmap

You could build this in the order the document presents it, but here is the order that gets you to a
*useful* thing fastest and then hardens it — each step a coherent unit of work.

**Stage 0 — the transformation core (hours, no infrastructure).**
Implement `models.py`, `transformer.py`, and `csv_writer.py`, plus their tests. This is pure,
dependency-free logic. You can already feed it sample documents and prove it produces the right CSV +
checksum. This is the smallest thing that demonstrates the *value* (correct flattening + integrity)
and it needs nothing running. *Deliverable:* given a list of hit dicts and a column map, get a
verified CSV.

**Stage 1 — read from a real Elasticsearch (a day).**
Add `errors.py`, `config.py`, `job_loader.py`, the `es_extract` extraction package, and `extractor.py`.
Point at a local ES (the compose stack), seed a job document and some data, and run
extract→transform→CSV end-to-end *without* Kafka or SFTP — just call the functions from a script.
You now have a working exporter; the trigger is "you ran the script." *Deliverable:* a script that
turns a job document into a verified CSV from real ES.

**Stage 2 — make it trustworthy (a day).**
Add `retry.py` and `validator.py`. Now the exporter refuses to produce a wrong file: it checks the
count, retries transient races, re-extracts once, and fails loudly otherwise. This is the step that
makes it production-*grade* rather than production-*shaped*. *Deliverable:* the exporter aborts on a
genuine count mismatch and recovers from a refresh race.

**Stage 3 — deliver it (half a day).**
Add `sftp_uploader.py` with strict host-key checking and retry. The exporter now lands the file at the
partner. *Deliverable:* CSV + sidecar arrive over SFTP and verify with `sha256sum -c`.

**Stage 4 — make it a service (a day).**
Add `logging_setup.py`, `control_consumer.py`, `pipeline.py`, and `__main__.py`. Now Kafka *triggers*
the work, offsets are committed only on success, and the thing runs as a long-lived daemon with
graceful shutdown. This is the MVP of the *product*: a control message in, a delivered file out.
*Deliverable:* `python -m etl` consuming the control topic end-to-end.

**Stage 5 — make extraction reusable and observable (half a day).**
Factor the ES extraction into the standalone `es_extract` package — PIT + `search_after`, with an
injectable `error_cls` so the host app keeps its own error taxonomy — and add the streaming NDJSON
diagnostic (`tee_to_ndjson`, opt-in via `ES_RAW_DUMP_DIR`). *Deliverable:* `from es_extract import
count, iter_hits` works standalone, and a job can dump its raw hits for troubleshooting.

**Stage 6 — the full product (ongoing, ticketed).**
These are the enhancements that turn a correct service into an operable one — and they map directly to
the backlog tickets:

- **CI:** run `ruff` + `mypy` + `pytest` on every PR with a coverage gate, so the safety net is
  enforced, not optional.
- **ES 9.x readiness:** migrate the deprecated `body=` calls (§20) to explicit kwargs and lift the
  version pin.
- **Dead-letter & alerting:** instead of bare `halt` on a hard failure (§13), route the failed job to
  a dead-letter topic and emit an alert, so a single bad job does not silently stop the line and
  someone is actually told.
- **Production security:** TLS/CA verification for ES, secrets from a vault rather than a flat `.env`,
  and key-file permission checks.
- **Observability:** per-job metrics (rows, durations, retry counts, failure rates) and optional
  OpenTelemetry spans across the stages, so the dashboards and the logs tell the same story.
- **Packaging & deployment:** a container image and a Helm/compose deployment with liveness and
  readiness probes.
- **Scaling:** verify and document multi-partition + multi-replica operation, so throughput grows by
  adding consumers rather than by rewriting anything.

The throughline of the roadmap is that **each stage produces something that works and is tested, and
later stages are additive.** You are never in a state where the thing is half-broken waiting for a
distant finish line. That is the practical reward of the layered, injected, pattern-driven design this
document has walked through: the system can be *grown*, because its parts are genuinely separable.

---

## Appendix: the modules at a glance

| Module | Responsibility | Key pattern / theory |
|---|---|---|
| `config.py` | env → validated immutable `Settings` | fail-fast config, immutability |
| `errors.py` | exception hierarchy | single catch boundary |
| `logging_setup.py` | structured JSON logs | structured-context logging |
| `retry.py` | backoff + jitter retry | injectable clock, retry-on-named-exception |
| `models.py` | `ControlMessage`, `JobSpec`, `CsvResult` | typed immutable data |
| `control_consumer.py` | Kafka control-topic wrapper | ports & adapters, at-least-once, manual commit |
| `__main__.py` | daemon loop & lifecycle | graceful shutdown, cumulative-commit correctness |
| `job_loader.py` | ES doc → `JobSpec` | data-driven jobs, validate-at-boundary |
| `extractor.py` | `_count` + PIT/`search_after` hits | thin seam over `es_extract`, injected `error_cls` |
| `transformer.py` | dotted-path projection | build-the-needed-abstraction, lazy streams |
| `csv_writer.py` | streaming CSV + SHA256 sidecar | decorator (tee'ing writer), end-to-end integrity |
| `validator.py` | two-tier count validation | fail-loud, inversion of control (callback) |
| `sftp_uploader.py` | `sftp -b` upload + retry | subprocess-over-library, strict host-key, no shell injection |
| `pipeline.py` | orchestrate one job | dependency injection, separation of concerns |
| `es_extract/pagination.py` | reusable PIT + `search_after` | resource lifecycle in `finally`, injectable `error_cls` |
| `es_extract/extract.py` | reusable `count` + `iter_hits` | dependency-light data-source port |
| `es_extract/diagnostics.py` | streaming NDJSON capture | tee'ing generator, memory-bounded |
