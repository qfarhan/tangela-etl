# Build-It-Yourself Tutorial

**Goal:** build the `kafka-es-csv-sftp-etl` service from an empty folder to a working daemon,
*one testable module at a time*. By the end you will have written every script yourself and you
will understand (a) what each function does, (b) the advanced Python techniques it uses, and
(c) the Kafka/Elasticsearch concepts behind every parameter.

**Who this is for:** a developer comfortable with Python who wants to level up on
*application* development — generators, protocols, dependency injection, decorators, structured
logging, subprocess hardening — while learning how Kafka consumers and Elasticsearch pagination
actually work. You should be able to type along and finish with running, tested code.

**Companion docs:** `docs/DESIGN.md` explains the *theory and patterns*; this is the *build log*.
Read them side by side if you like, but this one is self-contained.

---

## How to use this tutorial

The build is broken into **phases** (Jira *epics*), each containing **tickets**. Every ticket has:

- 🎯 **Goal** — what you are adding and why now.
- 📄 **The code** — the file(s) to write, in full. Type them; don't just read.
- 🐍 **Python deep-dive** — the advanced language features in play.
- 🔍 **Kafka/ES concept** — what the relevant parameters mean and why.
- 🧪 **Tests** — what to write to prove it works, and the command to run.
- ✅ **Definition of Done (milestone)** — the concrete, testable bar. When the listed command is
  green, that ticket is *done* and you can safely move on.

**The golden rule:** never start a ticket until the previous milestone is green. Each step builds on
a *verified* foundation, which is exactly how you keep a growing codebase from collapsing.

### Prerequisites

- Python **3.10+** (we use `str | None` syntax and modern generics).
- Docker Desktop (only needed from Phase C onward, for the local Kafka/ES/SFTP stack).
- The OpenSSH `sftp` client on your PATH (Phase D).

### One-time setup of the workspace

```bash
mkdir -p kafka-es-csv-sftp-etl/src/etl/pagination kafka-es-csv-sftp-etl/tests/pagination
cd kafka-es-csv-sftp-etl
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
```

We will fill in `pyproject.toml` in the very first ticket so `pip install -e` works.

---

# PHASE A — Foundations (Epic: ETL-A)

These five tickets give you a buildable, testable, type-checked skeleton plus the cross-cutting
primitives (errors, config, logging, retry) that *every* later module depends on. No Kafka or ES yet.

## 🎫 ETL-A1 — Project scaffolding & tooling  *(est. 0.5 d)*

🎯 **Goal:** an installable package with linting, typing, and testing wired up, so that from now on
every ticket can end with a green `pytest`.

📄 **`pyproject.toml`** (repo root):

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "kafka-es-csv-sftp-etl"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
    "confluent-kafka>=2.3",
    "elasticsearch>=8.0,<9",
    "python-dotenv>=1.0",
]

[project.optional-dependencies]
dev = ["pytest>=7.4", "pytest-mock>=3.12", "pytest-cov>=4.1", "ruff>=0.4", "mypy>=1.8"]

[project.scripts]
etl = "etl.__main__:main"

[tool.setuptools.packages.find]
where = ["src"]

[tool.pytest.ini_options]
testpaths = ["tests"]
pythonpath = ["src"]

[tool.ruff]
line-length = 100
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "SIM", "RUF"]
ignore = ["E501"]

[tool.mypy]
python_version = "3.10"
strict = true
mypy_path = "src"
```

Create empty `src/etl/__init__.py` and `src/etl/pagination/__init__.py`, then:

```bash
.venv/bin/pip install -e ".[dev]"
```

🐍 **Python deep-dive — the `src/` layout.** Putting code under `src/` means the *only* way to import
`etl` is to install it. This prevents the classic "it works on my machine because Python imported the
folder, not the installed package" bug. The `[project.scripts]` table creates a real `etl` command and
makes `python -m etl` work, turning a folder of files into an installable program. `[tool.ruff.lint]
select` turns on rule families: `E`/`F` (pyflakes/pycodestyle), `I` (import sorting), `B`
(bug-bear), `UP` (pyupgrade — modernizes syntax), `SIM` (simplify), `RUF` (ruff's own).

🧪 **Tests:** none yet — but verify the toolchain:

```bash
.venv/bin/ruff check src tests   # "All checks passed!" (no files yet is fine)
.venv/bin/pytest -q              # "no tests ran" is the expected success here
```

✅ **Definition of Done:** `pip install -e ".[dev]"` succeeds; `ruff`, `mypy`, and `pytest` all run
without configuration errors. You now have a green harness to build into.

---

## 🎫 ETL-A2 — The vocabulary: errors & models  *(est. 0.5 d)*

🎯 **Goal:** define the typed data and the exception hierarchy the whole pipeline speaks in. Pure
Python, no dependencies — the perfect first real code.

📄 **`src/etl/errors.py`:**

```python
from __future__ import annotations


class EtlError(Exception):
    """Base class for all ETL-pipeline errors."""


class ConfigError(EtlError): ...
class ControlMessageError(EtlError): ...
class JobSpecError(EtlError): ...
class ElasticsearchQueryError(EtlError): ...
class CsvWriteError(EtlError): ...
class SftpUploadError(EtlError): ...


class TransformError(EtlError):
    def __init__(self, message: str, *, job_id: str, hit_id: str | None = None,
                 jolt_op: str | None = None) -> None:
        super().__init__(message)
        self.job_id = job_id
        self.hit_id = hit_id
        self.jolt_op = jolt_op


class RecordCountMismatch(EtlError):
    def __init__(self, expected: int, actual: int, attempts: list[tuple[int, int]]) -> None:
        super().__init__(f"record count mismatch: expected={expected} actual={actual} "
                         f"attempts={attempts}")
        self.expected = expected
        self.actual = actual
        self.attempts = attempts


class RetryExhausted(EtlError):
    def __init__(self, attempts: int, last_exc: BaseException) -> None:
        super().__init__(f"retry exhausted after {attempts} attempts: {last_exc!r}")
        self.attempts = attempts
        self.last_exc = last_exc
```

📄 **`src/etl/models.py`:**

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ControlMessage:
    job_doc_id: str
    correlation_id: str | None
    raw_partition: int
    raw_offset: int


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    data_index: str
    query: dict[str, Any]
    column_paths: dict[str, str]
    columns: list[str]
    remote_filename: str


@dataclass(frozen=True)
class CsvResult:
    csv_path: Path
    sidecar_path: Path
    row_count: int
    sha256_hex: str
```

🐍 **Python deep-dive:**
- **`from __future__ import annotations`** makes every type annotation a *string* evaluated lazily,
  not at class-definition time. Benefits: forward references work without quotes, and import cost is
  lower. Put it at the top of *every* module.
- **A single base exception (`EtlError`)** is a classification tool. Later, one `except EtlError`
  catches every *expected* failure while letting genuine bugs (a `KeyError`) crash. Subclasses that
  need context (`TransformError`, `RecordCountMismatch`) override `__init__`, call `super().__init__`
  with a human message, and *attach structured attributes* so a failure is diagnosable from the
  exception object alone.
- **`@dataclass(frozen=True)`** auto-generates `__init__`, `__repr__`, and `__eq__`, and makes
  instances *immutable* (assigning a field raises `FrozenInstanceError`). Immutable data that flows
  between stages cannot be mutated behind your back — a huge simplification for reasoning and tests.

🧪 **Tests — `tests/test_models.py`:**

```python
from dataclasses import FrozenInstanceError

import pytest

from etl.errors import RecordCountMismatch
from etl.models import JobSpec


def test_jobspec_is_immutable():
    job = JobSpec("j", "idx", {"match_all": {}}, {}, ["a"], "out.csv")
    with pytest.raises(FrozenInstanceError):     # broad `Exception` would trip ruff B017
        job.data_index = "other"   # frozen dataclass forbids this


def test_record_count_mismatch_carries_context():
    err = RecordCountMismatch(expected=5, actual=4, attempts=[(5, 4)])
    assert err.expected == 5 and err.actual == 4
    assert "expected=5" in str(err)
```

✅ **Definition of Done:** `pytest -q` green; `from etl.models import JobSpec` and the exception
classes import cleanly; mypy clean.

---

## 🎫 ETL-A3 — Configuration  *(est. 0.5–1 d)*

🎯 **Goal:** read all settings from the environment once, validate them, and freeze them. A process
that starts has known-good config.

📄 **`src/etl/config.py`** (abridged to the teaching parts — the full file groups settings into
`KafkaConfig`, `EsConfig`, `PaginationConfig`, `RetryConfig`, `SftpConfig`, `Settings`):

```python
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from etl.errors import ConfigError


def _get(key: str, default: str | None = None, *, required: bool = False) -> str:
    val = os.environ.get(key, default)
    if required and (val is None or val == ""):
        raise ConfigError(f"missing required env var: {key}")
    return val if val is not None else ""


def _get_int(key: str, default: int) -> int:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise ConfigError(f"{key} must be an integer, got: {raw!r}") from e


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    control_topic: str
    group_id: str
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None

    def confluent_config(self) -> dict[str, str]:
        cfg: dict[str, str] = {
            "bootstrap.servers": self.bootstrap_servers,
            "group.id": self.group_id,
            "enable.auto.commit": "false",
            "auto.offset.reset": "earliest",
        }
        if self.security_protocol: cfg["security.protocol"] = self.security_protocol
        if self.sasl_mechanism:    cfg["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:     cfg["sasl.username"] = self.sasl_username
        if self.sasl_password:     cfg["sasl.password"] = self.sasl_password
        return cfg


# ... EsConfig, PaginationConfig, RetryConfig, SftpConfig, Settings dataclasses ...


def load_settings(*, dotenv_path: str | None = None) -> Settings:
    load_dotenv(dotenv_path=dotenv_path, override=False)
    kafka = KafkaConfig(
        bootstrap_servers=_get("KAFKA_BOOTSTRAP_SERVERS", required=True),
        control_topic=_get("KAFKA_CONTROL_TOPIC", required=True),
        group_id=_get("KAFKA_GROUP_ID", required=True),
        # optional SASL fields ...
    )
    pagination_strategy = _get("PAGINATION_STRATEGY", "scroll").lower()
    if pagination_strategy not in ("scroll", "search_after"):
        raise ConfigError(f"PAGINATION_STRATEGY must be 'scroll' or 'search_after', "
                          f"got {pagination_strategy!r}")
    # ... build EsConfig, PaginationConfig, RetryConfig, SftpConfig, Settings ...
    return Settings(...)
```

🐍 **Python deep-dive:**
- **Fail-fast helpers.** `_get(..., required=True)` converts "missing" into a precise `ConfigError`
  *at startup*, not a confusing `None` deep in a loop. `_get_int` wraps `int()` and **re-raises with
  context** using `raise ... from e` — *exception chaining*, which preserves the original `ValueError`
  as `__cause__` so tracebacks show the full story.
- **A method on the config object** (`confluent_config`) keeps the translation to Kafka's dict format
  next to the data and makes `"enable.auto.commit": "false"` impossible to forget (more on why in
  ETL-E1).

📨 **Kafka concept — the consumer config keys:**
- **`bootstrap.servers`** — one or more broker `host:port` entries the client contacts first to learn
  the cluster topology.
- **`group.id`** — the *consumer group*. Kafka divides a topic's partitions among members of a group
  and tracks committed offsets *per group*. Two processes with the same `group.id` share the work;
  different groups each get the full stream.
- **`enable.auto.commit=false`** — we will commit offsets *manually* (ETL-E1/E3). This is the linchpin
  of at-least-once delivery.
- **`auto.offset.reset=earliest`** — where a brand-new group with no committed offset starts: from the
  beginning of the topic. The safe default for a job runner (don't miss pending work).

🧪 **Tests — `tests/test_config.py`** (note the *hermeticity* trick):

```python
import pytest
from etl.config import load_settings
from etl.errors import ConfigError

_BASE_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092", "KAFKA_CONTROL_TOPIC": "ctl",
    "KAFKA_GROUP_ID": "g", "ES_HOSTS": "http://localhost:9200", "ES_JOB_INDEX": "jobs",
    "SFTP_HOST": "h", "SFTP_USER": "u", "SFTP_KEY_PATH": "/k", "SFTP_KNOWN_HOSTS": "/kh",
}

def _set_env(mp, env):
    for k in list(env) + list(_BASE_ENV):
        mp.delenv(k, raising=False)
    for k, v in env.items():
        mp.setenv(k, v)

def test_missing_required_raises(monkeypatch):
    env = dict(_BASE_ENV); env.pop("KAFKA_BOOTSTRAP_SERVERS")
    _set_env(monkeypatch, env)
    with pytest.raises(ConfigError, match="KAFKA_BOOTSTRAP_SERVERS"):
        load_settings()
```

And in **`tests/conftest.py`**, an autouse fixture that makes config tests hermetic:

```python
import pytest

@pytest.fixture(autouse=True)
def _isolate_dotenv(monkeypatch):
    # load_dotenv() walks up the tree and would load a developer's local .env
    # (e.g. from `cp .env.local .env`), repopulating vars a test removed. Disable it.
    monkeypatch.setattr("etl.config.load_dotenv", lambda *a, **k: False)
```

🐍 **Python deep-dive — hidden inputs.** `load_dotenv()` reads the *filesystem* — an ambient input
your test didn't pass in. A test that depends on the *absence* of a file is not hermetic. The autouse
fixture neutralizes it. **Rule:** anything that reads ambient state (clock, filesystem, env) is a
hidden input — make it injectable or neutralize it in tests. (This is a real bug we hit; learn it
cheaply here.)

✅ **Definition of Done:** `pytest tests/test_config.py -q` green, including the "missing required"
and "bad integer" cases; `mypy` clean. Settings load and validate.

---

## 🎫 ETL-A4 — Structured logging  *(est. 0.5 d)*

🎯 **Goal:** emit one JSON object per log line so logs are both human-readable and machine-queryable,
with per-job context attached.

📄 **`src/etl/logging_setup.py`:**

```python
from __future__ import annotations

import json
import logging
import sys
from typing import Any

_RESERVED = {  # standard LogRecord attributes we don't want to duplicate
    "args", "asctime", "created", "exc_info", "exc_text", "filename", "funcName",
    "levelname", "levelno", "lineno", "message", "module", "msecs", "msg", "name",
    "pathname", "process", "processName", "relativeCreated", "stack_info",
    "thread", "threadName", "taskName",
}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _RESERVED and not key.startswith("_"):
                payload[key] = value          # merge `extra={...}` fields
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    for h in list(root.handlers):
        root.removeHandler(h)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level)
```

🐍 **Python deep-dive:**
- **A `LogRecord` is just an object with a `__dict__`.** When you call
  `log.info("msg", extra={"job_id": "x"})`, Python sets `record.job_id = "x"`. Our formatter walks
  `record.__dict__`, skips the *reserved* built-in attributes, and merges the rest — that's how
  arbitrary context becomes queryable JSON fields without changing the message string. This is the
  **structured-context logging** pattern.
- **`json.dumps(..., default=str)`** guarantees serialization never crashes: any non-JSON value
  (a `Path`, a `datetime`) falls back to `str()`. It also *escapes newlines*, which prevents **log
  forging** (an attacker injecting fake log lines through a crafted field).
- **Logging to stdout** is correct for containers — the orchestrator captures and ships it. The app
  should not manage log files.

🧪 **Tests — `tests/test_logging.py`:**

```python
import json
import logging

from etl.logging_setup import configure_logging

def test_extra_fields_become_json(capsys):
    configure_logging("INFO")
    logging.getLogger("t").info("hello", extra={"job_id": "abc"})
    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["msg"] == "hello" and payload["job_id"] == "abc"
```

✅ **Definition of Done:** logs parse as JSON and carry `extra` fields; test green.

---

## 🎫 ETL-A5 — The retry primitive  *(est. 1 d)*

🎯 **Goal:** one reusable exponential-backoff-with-jitter helper that Kafka/ES/SFTP all build on,
*with an injectable clock so tests spend zero real time*. This is the richest "advanced Python"
ticket in Phase A — take your time.

📄 **`src/etl/retry.py`:**

```python
from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

from etl.errors import RetryExhausted

T = TypeVar("T")
_log = logging.getLogger(__name__)


def _compute_delay(attempt: int, base: float, cap: float, jitter: float,
                   rng: random.Random) -> float:
    bounded: float = min(cap, base * (2 ** attempt))
    if jitter > 0:
        bounded *= 1.0 + rng.uniform(-jitter, jitter)
    return max(0.0, bounded)


def retry_call(
    fn: Callable[..., T], *args: Any,
    on: tuple[type[BaseException], ...],
    attempts: int = 5, base: float = 1.0, cap: float = 30.0, jitter: float = 0.25,
    sleeper: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    wrap_final: bool = False,
    log_extra: dict[str, Any] | None = None,
    **kwargs: Any,
) -> T:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    rng = rng or random.Random()
    last: BaseException | None = None
    for attempt in range(attempts):
        try:
            return fn(*args, **kwargs)
        except on as exc:
            last = exc
            if attempt == attempts - 1:
                break
            delay = _compute_delay(attempt, base, cap, jitter, rng)
            _log.warning("retry: %s attempt=%d/%d delay=%.3fs err=%r",
                         getattr(fn, "__name__", repr(fn)), attempt + 1, attempts, delay, exc,
                         extra={**(log_extra or {}), "retry_attempt": attempt + 1})
            sleeper(delay)
    assert last is not None
    if wrap_final:
        raise RetryExhausted(attempts, last) from last
    raise last
```

(The repo also includes a `@retry(...)` *decorator* form that wraps `retry_call` using
`functools.wraps`.)

🐍 **Python deep-dive:**
- **`TypeVar("T")` + `Callable[..., T] -> T`.** This makes `retry_call` *generic*: it returns whatever
  type `fn` returns. mypy preserves the type through the retry wrapper — `retry_call(get_int, ...)`
  is typed `int`. Generics are how you write reusable code without losing type safety.
- **`except on as exc` where `on: tuple[type[BaseException], ...]`.** `except` accepts a *tuple* of
  exception types. By passing the tuple in, the *caller* decides what is retryable. A bug (e.g.
  `TypeError`) is *not* in the tuple, so it propagates immediately — you never retry bugs.
- **Dependency injection of the clock and RNG.** `sleeper=time.sleep` and `rng=random.Random()` are
  parameters. Production uses the real ones; tests pass `sleeper=lambda _: None` (instant) and a
  seeded `rng` (deterministic jitter). This single design choice is what makes retry logic *fast and
  deterministic to test* — otherwise a 5-retry test would sleep for real seconds.
- **Re-raise the original.** After the last attempt we `raise last` — the meaningful
  `SftpUploadError`, not a generic wrapper — so callers can still catch the specific type.
- **`*args`/`**kwargs` passthrough** lets `retry_call` wrap *any* function signature.
- **The exponential+jitter formula** (`_compute_delay`): `base * 2**attempt` grows the wait
  (1s, 2s, 4s…), `min(cap, …)` bounds it, and `* (1 ± jitter)` *desynchronizes* many clients so a
  recovering service isn't hit by a synchronized "thundering herd."

🧪 **Tests — `tests/test_retry.py`** (no real time spent):

```python
import pytest
from etl.retry import retry_call

class Boom(Exception): ...

def test_succeeds_first_try_no_sleep():
    sleeps = []
    assert retry_call(lambda: 42, on=(Boom,), sleeper=sleeps.append) == 42
    assert sleeps == []

def test_fails_then_succeeds_counts_sleeps():
    calls = {"n": 0}; sleeps = []
    def flaky():
        calls["n"] += 1
        if calls["n"] < 3: raise Boom()
        return "ok"
    assert retry_call(flaky, on=(Boom,), sleeper=sleeps.append, jitter=0.0) == "ok"
    assert calls["n"] == 3 and len(sleeps) == 2        # slept before attempts 2 and 3

def test_exhaustion_reraises_original():
    def always(): raise Boom("nope")
    with pytest.raises(Boom, match="nope"):
        retry_call(always, on=(Boom,), attempts=4, sleeper=lambda _: None)
```

✅ **Definition of Done:** `pytest tests/test_retry.py -q` green and *instant* (no real sleeping);
mypy clean. 🏁 **PHASE A MILESTONE:** `ruff check src tests`, `mypy src`, and `pytest -q` are all
green. You have a typed, tested foundation.

---

# PHASE B — The transformation core (Epic: ETL-B)

The most valuable code with the *least* infrastructure: turn nested JSON into flat, verified CSV.
Everything here is pure and offline — you can demo real value before touching Kafka or ES.

## 🎫 ETL-B1 — The transformer (dotted-path projection)  *(est. 1 d)*

🎯 **Goal:** flatten each Elasticsearch document into a row of named columns using a small path
language (`a.b[0].c`), with missing paths becoming empty strings.

📄 **`src/etl/transformer.py`:**

```python
from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from typing import Any

from etl.errors import TransformError

_TOKEN_RE = re.compile(r"\[\d+\]|[^.\[]+")   # "users[0].name" -> ["users", "[0]", "name"]
_INDEX_RE = re.compile(r"\[(\d+)\]")


def _tokens(path: str) -> list[str]:
    return _TOKEN_RE.findall(path)


def get_by_path(doc: Any, path: str) -> Any:
    """Resolve a dotted path inside `doc`. Returns "" when any step is missing."""
    cur: Any = doc
    for tok in _tokens(path):
        idx_match = _INDEX_RE.fullmatch(tok)
        if idx_match is not None:
            idx = int(idx_match.group(1))
            if isinstance(cur, list) and 0 <= idx < len(cur):
                cur = cur[idx]
            else:
                return ""
        else:
            if isinstance(cur, dict) and tok in cur:
                cur = cur[tok]
            else:
                return ""
        if cur is None:
            return ""
    return cur


def project(hit: dict[str, Any], columns: list[str], column_paths: dict[str, str] | None,
            *, job_id: str, hit_id: str | None = None) -> dict[str, Any]:
    paths = column_paths or {}
    out: dict[str, Any] = {}
    for col in columns:
        path = paths.get(col, col)            # default: the column maps to its own name
        if not path:
            raise TransformError(f"empty path for column {col!r}", job_id=job_id, hit_id=hit_id)
        out[col] = get_by_path(hit, path)
    return out


def iter_transformed(hits: Iterable[dict[str, Any]], column_paths: dict[str, str] | None,
                     columns: list[str], *, job_id: str) -> Iterator[dict[str, Any]]:
    for hit in hits:
        hit_id = hit.get("_id") if isinstance(hit, dict) else None
        yield project(hit, columns, column_paths, job_id=job_id, hit_id=hit_id)
```

🐍 **Python deep-dive:**
- **Generators (`yield`) and `Iterable` vs `Iterator`.** `iter_transformed` is a *generator
  function*: calling it returns a lazy `Iterator` that produces one row at a time as the consumer
  pulls. It accepts any `Iterable` (a list, another generator, an ES stream). This is the core of
  *streaming*: a million hits never sit in memory at once. Memory is bounded by one row.
- **`isinstance` type-guards before indexing.** At each step we confirm the current value is a
  `dict`/`list` before we key/index into it — turning "ragged data" into an empty cell instead of an
  exception. This is defensive parsing of untrusted shapes.
- **Fail loud only on *operator* error.** A configured-but-empty path raises `TransformError`
  (someone mis-wrote the job doc); missing *data* does not. Knowing *which* failures to surface vs.
  tolerate is a design skill.

🧪 **Tests — `tests/test_transformer.py`:**

```python
import pytest
from etl.errors import TransformError
from etl.transformer import get_by_path, project

DOC = {"a": {"b": "x"}, "items": [{"sku": "s1"}], "_id": "h1"}

@pytest.mark.parametrize("path,expected", [
    ("a.b", "x"), ("items[0].sku", "s1"),
    ("a.missing", ""), ("items[5].sku", ""), ("nope", ""),
])
def test_get_by_path(path, expected):
    assert get_by_path(DOC, path) == expected

def test_project_uses_paths_and_defaults():
    row = project({"id": "1", "u": {"n": "Al"}}, ["id", "name"],
                  {"name": "u.n"}, job_id="j")
    assert row == {"id": "1", "name": "Al"}     # "id" defaulted to its own name

def test_empty_path_raises():
    with pytest.raises(TransformError):
        project(DOC, ["c"], {"c": ""}, job_id="j", hit_id="h1")
```

✅ **Definition of Done:** `pytest tests/test_transformer.py -q` green, including the
parametrized missing-path cases.

---

## 🎫 ETL-B2 — CSV writer + integrity sidecar  *(est. 0.5–1 d)*

🎯 **Goal:** stream rows to a CSV file and compute a SHA256 checksum *in the same pass*, writing a
`sha256sum`-format sidecar.

📄 **`src/etl/csv_writer.py`:**

```python
from __future__ import annotations

import csv
import hashlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any, BinaryIO

from etl.errors import CsvWriteError
from etl.models import CsvResult


class _HashingWriter:
    """A file-like wrapper that mirrors every write into a hashlib hasher."""
    def __init__(self, fh: BinaryIO, hasher: Any) -> None:
        self._fh = fh
        self._hasher = hasher
    def write(self, s: str) -> int:
        b = s.encode("utf-8")
        self._fh.write(b)
        self._hasher.update(b)
        return len(s)


def write_csv(rows: Iterable[dict[str, Any]], columns: list[str], csv_path: Path) -> CsvResult:
    if not columns:
        raise CsvWriteError("columns must not be empty")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    hasher = hashlib.sha256()
    row_count = 0
    try:
        with csv_path.open("wb") as fh:
            writer = csv.DictWriter(_HashingWriter(fh, hasher), fieldnames=columns,
                                    extrasaction="ignore", lineterminator="\n")
            writer.writeheader()
            for row in rows:
                writer.writerow({c: _stringify(row.get(c, "")) for c in columns})
                row_count += 1
    except OSError as e:
        raise CsvWriteError(f"failed writing csv {csv_path}: {e!r}") from e

    sidecar_path = csv_path.with_suffix(csv_path.suffix + ".sha256")
    digest = hasher.hexdigest()
    sidecar_path.write_text(f"{digest}  {csv_path.name}\n", encoding="utf-8")
    return CsvResult(csv_path=csv_path, sidecar_path=sidecar_path,
                     row_count=row_count, sha256_hex=digest)


def _stringify(v: Any) -> str:
    if v is None: return ""
    if isinstance(v, bool): return "true" if v else "false"
    return str(v)
```

🐍 **Python deep-dive:**
- **Duck typing + the decorator pattern at the I/O level.** `csv.DictWriter` calls `.write(str)` on
  whatever you give it — it doesn't care about the type, only the method (*duck typing*).
  `_HashingWriter` provides `write`, forwards bytes to the real file, *and* feeds them to the hasher.
  Result: the file is hashed **once, incrementally, as it's written** — no second read pass. This is
  the decorator pattern (wrap an object to add behavior transparently).
- **Context manager `with csv_path.open("wb") as fh:`** guarantees the file is flushed and closed even
  if an exception is raised mid-write. Always manage files (and sockets, locks) with `with`.
- **Binary mode + `lineterminator="\n"`** make the bytes — and therefore the hash — identical across
  platforms (no `\r\n` surprises on Windows).
- **`extrasaction="ignore"`** drops dict keys not in `fieldnames` instead of raising — robust to
  rows carrying extra fields.

🧪 **Tests — `tests/test_csv_writer.py`:**

```python
import hashlib
from etl.csv_writer import write_csv

def test_round_trip_and_sidecar(tmp_path):
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    out = tmp_path / "f.csv"
    res = write_csv(rows, ["a", "b"], out)
    assert res.row_count == 2
    text = out.read_text()
    assert text.splitlines()[0] == "a,b"            # header + column order
    # sidecar hash matches an independent recompute (what `sha256sum -c` does):
    assert res.sha256_hex == hashlib.sha256(out.read_bytes()).hexdigest()
    assert res.sidecar_path.read_text().strip() == f"{res.sha256_hex}  f.csv"
```

✅ **Definition of Done:** `pytest tests/test_csv_writer.py -q` green; the recomputed hash matches
the sidecar. 🏁 **PHASE B MILESTONE (Stage 0):** you can now flatten sample documents into a verified
CSV with *zero infrastructure*. Write a tiny `scripts/prototype.py` that feeds hard-coded sample
hits through `iter_transformed` → `write_csv` and prints the result; running it is your first
end-to-end demo. **This is a shippable, demoable unit of value.**

---

# PHASE C — Elasticsearch (Epic: ETL-C)

Now connect to the data plane: page through a query efficiently and turn a job document into a typed
`JobSpec`.

## 🎫 ETL-C1 — Pagination strategies + extractor  *(est. 1.5–2 d)*

🎯 **Goal:** stream every hit of a query out of Elasticsearch without loading it all into memory or
hitting deep-pagination limits — via a *pluggable* strategy (Scroll for NiFi parity, `search_after`
for new work).

🔍 **ES concept — why not `from`/`size`?** Naive offset pagination ("rows 1,000,000–1,000,999") forces
ES to find and discard the first million on every shard *per request* (cost grows with depth; ES caps
it via `index.max_result_window`, default 10,000), and it can skip/duplicate rows if data changes
mid-scan. The fix is a **consistent point-in-time view** plus a **stable cursor**. Two mechanisms:

- **Scroll API:** `search(scroll="5m", size=N, ...)` opens a frozen view and returns a `_scroll_id`;
  you call `scroll(scroll_id=..., scroll="5m")` repeatedly until pages run dry, then `clear_scroll`.
  - `scroll="5m"` is the *keep-alive TTL* — how long ES retains the context between calls.
  - `size=N` is the page size (hits per request).
  - `_scroll_id` is the server-side cursor handle.
- **PIT + `search_after` (modern):** `open_point_in_time(index, keep_alive="5m")` → a lightweight
  named view; then stateless `search` calls that `sort` by a stable key and pass `search_after` =
  the previous page's last hit's sort values. Close with `close_point_in_time`.
  - `sort: [{"_shard_doc": "asc"}]` — the cheapest stable tie-breaker, available *only inside a PIT*.
  - `track_total_hits: False` — skip the expensive exact-total computation we don't need.

📄 **`src/etl/pagination/base.py`** (the interface + factory):

```python
from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Protocol

from etl.errors import ConfigError


class PaginationStrategy(Protocol):
    def iter_hits(self, *, es: Any, index: str, query: dict[str, Any],
                  page_size: int) -> Iterator[dict[str, Any]]: ...


def make_strategy(name: str, *, keep_alive: str) -> PaginationStrategy:
    name = name.lower()
    if name == "scroll":
        from etl.pagination.scroll import ScrollPagination
        return ScrollPagination(keep_alive=keep_alive)
    if name == "search_after":
        from etl.pagination.search_after import SearchAfterPagination
        return SearchAfterPagination(keep_alive=keep_alive)
    raise ConfigError(f"unknown pagination strategy: {name!r}")
```

📄 **`src/etl/pagination/scroll.py`:**

```python
from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from etl.errors import ElasticsearchQueryError

_log = logging.getLogger(__name__)


@dataclass
class ScrollPagination:
    keep_alive: str = "5m"

    def iter_hits(self, *, es: Any, index: str, query: dict[str, Any],
                  page_size: int) -> Iterator[dict[str, Any]]:
        scroll_id: str | None = None
        try:
            try:
                resp = es.search(index=index, scroll=self.keep_alive, size=page_size,
                                 body={"query": query})
            except Exception as e:
                raise ElasticsearchQueryError(f"initial scroll search failed: {e!r}") from e
            scroll_id = resp.get("_scroll_id")
            hits = resp.get("hits", {}).get("hits", [])
            while hits:
                for h in hits:
                    yield h.get("_source", {})
                if scroll_id is None:
                    break
                try:
                    resp = es.scroll(scroll_id=scroll_id, scroll=self.keep_alive)
                except Exception as e:
                    raise ElasticsearchQueryError(f"scroll continuation failed: {e!r}") from e
                scroll_id = resp.get("_scroll_id")
                hits = resp.get("hits", {}).get("hits", [])
        finally:
            if scroll_id is not None:
                try:
                    es.clear_scroll(scroll_id=scroll_id)
                except Exception as e:                  # best-effort: the scroll TTL reaps it anyway
                    _log.warning("clear_scroll failed: %r", e)
```

📄 **`src/etl/pagination/search_after.py`** (same shape; PIT + cursor — see repo for the full file):

```python
        pit = es.open_point_in_time(index=index, keep_alive=self.keep_alive)
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
                hits = resp.get("hits", {}).get("hits", [])
                if not hits:
                    break
                for h in hits:
                    yield h.get("_source", {})
                search_after = hits[-1].get("sort")    # the cursor for the next page
        finally:
            if pit_id is not None:
                es.close_point_in_time(body={"id": pit_id})
```

📄 **`src/etl/extractor.py`:**

```python
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from etl.errors import ElasticsearchQueryError
from etl.models import JobSpec
from etl.pagination.base import PaginationStrategy


def expected_count(es: Any, index: str, query: dict[str, Any]) -> int:
    try:
        resp = es.count(index=index, body={"query": query})
    except Exception as e:
        raise ElasticsearchQueryError(f"count failed: {e!r}") from e
    return int(resp.get("count", 0))


def iter_hits(es: Any, job: JobSpec, strategy: PaginationStrategy, *,
              page_size: int) -> Iterator[dict[str, Any]]:
    return strategy.iter_hits(es=es, index=job.data_index, query=job.query, page_size=page_size)
```

🐍 **Python deep-dive:**
- **`typing.Protocol` = structural typing.** `PaginationStrategy` defines a *shape*
  (`iter_hits(...)`); any class with that method satisfies it — *no inheritance required*. This is the
  Pythonic Strategy pattern: the two implementations don't import or subclass the protocol, they just
  match it. The factory `make_strategy` is the single place that knows the set of strategies (local
  imports avoid import cycles).
- **Resource-owning generators clean up in `finally`.** A scroll context / PIT is a *server-side
  resource*. The `try/finally` runs `clear_scroll` / `close_point_in_time` **even if the consumer
  abandons iteration early** (e.g., a downstream stage raises and Python closes the generator). This
  is the make-or-break correctness property — forget it and you leak ES resources.
- **Swallow cleanup errors.** If `clear_scroll` itself fails we *log and continue* — raising from
  cleanup would mask the real exception that triggered it.

🔍 **ES concept — the response shape.** A search response is `{"hits": {"hits": [ {"_source": {...},
"_id": "...", "sort": [...]}, ... ]}}`. The actual document is in `_source`; `_id` is its id; `sort`
(present when you sort) is the cursor for `search_after`. `_count` returns `{"count": N}`.

🧪 **Tests — `tests/pagination/test_scroll.py`** (fake ES, assert cleanup-on-early-exit):

```python
from etl.pagination.scroll import ScrollPagination

class FakeES:
    def __init__(self): self.cleared = False
    def search(self, **kw): return {"_scroll_id": "s1",
        "hits": {"hits": [{"_source": {"n": 1}}, {"_source": {"n": 2}}]}}
    def scroll(self, **kw): return {"_scroll_id": "s1", "hits": {"hits": []}}
    def clear_scroll(self, **kw): self.cleared = True

def test_scroll_streams_then_clears():
    es = FakeES()
    out = list(ScrollPagination().iter_hits(es=es, index="i", query={}, page_size=2))
    assert out == [{"n": 1}, {"n": 2}] and es.cleared is True

def test_scroll_clears_even_on_early_break():
    es = FakeES()
    gen = ScrollPagination().iter_hits(es=es, index="i", query={}, page_size=2)
    next(gen); gen.close()              # abandon iteration after one hit
    assert es.cleared is True           # finally still ran
```

✅ **Definition of Done:** both pagination tests green, *especially* the early-`close()` cleanup test;
mypy clean.

---

## 🎫 ETL-C2 — Job loader (validate at the boundary)  *(est. 0.5 d)*

🎯 **Goal:** fetch a job document by id and validate every field into a trustworthy `JobSpec`.

📄 **`src/etl/job_loader.py`:**

```python
from __future__ import annotations

from typing import Any

from etl.errors import ElasticsearchQueryError, JobSpecError
from etl.models import JobSpec

_REQUIRED = ("data_index", "query", "columns", "remote_filename")


def load_job(es: Any, *, job_index: str, job_doc_id: str) -> JobSpec:
    try:
        doc = es.get(index=job_index, id=job_doc_id)
    except Exception as e:
        raise ElasticsearchQueryError(f"failed to GET job doc {job_doc_id}: {e!r}") from e

    if not doc.get("found", True):          # 8.x get() can return {"found": false} instead of raising
        raise JobSpecError(f"job doc {job_doc_id} not found in {job_index}")
    source = doc.get("_source") or {}
    missing = [f for f in _REQUIRED if f not in source]
    if missing:
        raise JobSpecError(f"job doc {job_doc_id} missing fields: {missing}")

    query = source["query"]
    if not isinstance(query, dict):
        raise JobSpecError(f"job doc {job_doc_id}: 'query' must be an object")
    columns = source["columns"]
    if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
        raise JobSpecError(f"job doc {job_doc_id}: 'columns' must be list[str]")
    column_paths = source.get("column_paths") or {}
    if not isinstance(column_paths, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in column_paths.items()
    ):
        raise JobSpecError(f"job doc {job_doc_id}: 'column_paths' must be dict[str, str]")

    remote_filename = source["remote_filename"]
    if not isinstance(remote_filename, str) or not remote_filename:
        raise JobSpecError(f"job doc {job_doc_id}: 'remote_filename' must be a non-empty string")

    return JobSpec(
        job_id=str(source.get("job_id", job_doc_id)),
        data_index=str(source["data_index"]),
        query=query, column_paths=column_paths,
        columns=list(columns), remote_filename=remote_filename,
    )
```

🐍 **Python deep-dive — validate-at-the-boundary.** A job document is *external, untyped* data an
operator wrote. The moment it enters the typed core, we check every field — types included — and
convert failures into a precise `JobSpecError`. After `load_job` returns, the rest of the pipeline
treats `JobSpec` as trustworthy and never re-checks. All suspicion is concentrated in one place. Note
`all(isinstance(...) for ...)` — a *generator expression* inside `all()` for a concise "every element
is a string" check.

🔍 **ES concept — `es.get(index, id)`** does a direct primary-key lookup (fast, by document id),
returning `{"_source": {...}, "found": true, ...}`. A missing id raises `NotFoundError` in the 8.x
client, which we wrap.

🧪 **Tests — `tests/test_job_loader.py`:**

```python
import pytest
from etl.errors import JobSpecError
from etl.job_loader import load_job

class FakeES:
    def __init__(self, src): self._src = src
    def get(self, **kw): return {"_source": self._src, "found": True}

def test_valid_doc():
    es = FakeES({"data_index": "d", "query": {"match_all": {}},
                 "columns": ["a"], "remote_filename": "o.csv"})
    job = load_job(es, job_index="jobs", job_doc_id="j1")
    assert job.data_index == "d" and job.columns == ["a"]

def test_missing_field_raises():
    es = FakeES({"data_index": "d"})       # missing query/columns/remote_filename
    with pytest.raises(JobSpecError, match="missing fields"):
        load_job(es, job_index="jobs", job_doc_id="j1")
```

✅ **Definition of Done:** job-loader tests green. 🏁 **PHASE C MILESTONE:** with the local ES stack
up (Phase F) and a seeded job, you can extract real documents end-to-end. The `--live` mode of
`scripts/prototype.py` (`load_job → expected_count → make_strategy → iter_hits → write_csv`) now
works. Extraction is *complete and testable*.

---

# PHASE D — Trust & delivery (Epic: ETL-D)

## 🎫 ETL-D1 — The validator (two-tier count check)  *(est. 1–1.5 d)*

🎯 **Goal:** guarantee the CSV row count equals what ES says — tolerating transient races, but failing
loudly on real loss.

🔍 **ES concept — near-real-time & refresh races.** Elasticsearch is *near*-real-time: newly indexed
docs become searchable only after a *refresh* (~1s by default). `_count` and a paginated read are
separate operations that can observe the index a moment apart. So a *transient* mismatch can be a
refresh race, not data loss. We must not cry wolf on the first mismatch — nor ship a wrong file.

📄 **`src/etl/validator.py`:**

```python
from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import Any

from etl.config import RetryConfig
from etl.errors import RecordCountMismatch
from etl.extractor import expected_count
from etl.models import CsvResult

_log = logging.getLogger(__name__)


def validate_counts(expected: int, actual: int) -> None:
    if expected != actual:
        raise RecordCountMismatch(expected=expected, actual=actual, attempts=[(expected, actual)])


def validate_with_retry(*, es: Any, index: str, query: dict[str, Any], csv_result: CsvResult,
                        retry_cfg: RetryConfig, on_full_reextract: Callable[[], CsvResult],
                        sleeper: Callable[[float], None] | None = None,
                        rng: random.Random | None = None,
                        log_extra: dict[str, Any] | None = None) -> CsvResult:
    sleeper = sleeper or time.sleep
    rng = rng or random.Random()
    attempts_log: list[tuple[int, int]] = []
    current = csv_result

    # Tier 1: re-query _count up to N times (handles refresh races).
    for attempt in range(retry_cfg.max_attempts):
        es_count = expected_count(es, index, query)
        attempts_log.append((es_count, current.row_count))
        if es_count == current.row_count:
            return current
        if attempt == retry_cfg.max_attempts - 1:
            break
        delay = min(retry_cfg.backoff_cap, retry_cfg.backoff_base * (2 ** attempt))
        if retry_cfg.jitter > 0:
            delay *= 1.0 + rng.uniform(-retry_cfg.jitter, retry_cfg.jitter)
        sleeper(max(0.0, delay))

    # Tier 2: one full extract+CSV re-run, then compare to a fresh _count.
    current = on_full_reextract()
    final = expected_count(es, index, query)
    attempts_log.append((final, current.row_count))
    if final != current.row_count:
        raise RecordCountMismatch(expected=final, actual=current.row_count, attempts=attempts_log)
    return current
```

🐍 **Python deep-dive:**
- **Inversion of control via a callback.** The validator doesn't know *how* to re-extract — that would
  couple it to the extractor, the strategy, and the CSV writer. It accepts
  `on_full_reextract: Callable[[], CsvResult]` and *calls back* into the orchestrator. The validator
  stays focused and unit-testable with a fake callback.
- **Why this loop is hand-rolled (not `retry_call`).** `retry_call` retries on an *exception*; here we
  retry on a *value comparison* (`es_count == row_count`). Recognizing that the existing abstraction
  doesn't fit — and writing a small purpose-built loop instead — is a real engineering judgment.
- **Injectable `sleeper`/`rng`** again, for instant deterministic tests.

🧪 **Tests — `tests/test_validator.py`:**

```python
from etl.config import RetryConfig
from etl.validator import validate_with_retry
from etl.models import CsvResult
from pathlib import Path
import pytest
from etl.errors import RecordCountMismatch

CFG = RetryConfig(max_attempts=5, backoff_base=0.0, backoff_cap=0.0, jitter=0.0)
def _csv(n): return CsvResult(Path("f.csv"), Path("f.csv.sha256"), n, "deadbeef")

class CountES:
    def __init__(self, counts): self._counts = list(counts)
    def count(self, **kw): return {"count": self._counts.pop(0)}

def test_matches_first_try():
    es = CountES([3])
    assert validate_with_retry(es=es, index="i", query={}, csv_result=_csv(3),
        retry_cfg=CFG, on_full_reextract=lambda: _csv(3), sleeper=lambda _: None).row_count == 3

def test_flaps_then_matches():
    es = CountES([2, 2, 3])                  # races, then settles
    res = validate_with_retry(es=es, index="i", query={}, csv_result=_csv(3),
        retry_cfg=CFG, on_full_reextract=lambda: _csv(3), sleeper=lambda _: None)
    assert res.row_count == 3

def test_reextract_then_fail():
    es = CountES([9, 9, 9, 9, 9, 9])         # never matches 3; reextract still 3
    called = {"n": 0}
    def reextract():
        called["n"] += 1; return _csv(3)
    with pytest.raises(RecordCountMismatch):
        validate_with_retry(es=es, index="i", query={}, csv_result=_csv(3),
            retry_cfg=CFG, on_full_reextract=reextract, sleeper=lambda _: None)
    assert called["n"] == 1                   # re-extract attempted exactly once
```

✅ **Definition of Done:** validator tests green (match / flap-then-match / reextract-then-fail);
mypy clean.

---

## 🎫 ETL-D2 — SFTP uploader  *(est. 1 d)*

🎯 **Goal:** deliver the CSV + sidecar over SFTP securely, with retry — by shelling out to the system
`sftp` binary (no paramiko), enforcing strict host-key checking.

📄 **`src/etl/sftp_uploader.py`:**

```python
from __future__ import annotations

import logging
import shlex
import subprocess
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from etl.config import RetryConfig, SftpConfig
from etl.errors import SftpUploadError
from etl.retry import retry_call

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class UploadPlan:
    local: Path
    remote: str


def _build_batch(plans: list[UploadPlan]) -> str:
    lines = [f"put {shlex.quote(str(p.local))} {shlex.quote(p.remote)}" for p in plans]
    lines.append("bye")
    return "\n".join(lines) + "\n"


def _run_sftp(cfg: SftpConfig, batch_text: str, *, timeout: float) -> None:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sftpbatch", delete=True,
                                     encoding="utf-8") as batch_fh:
        batch_fh.write(batch_text); batch_fh.flush()
        argv = ["sftp", "-b", batch_fh.name, "-i", str(cfg.key_path), "-P", str(cfg.port),
                "-o", f"UserKnownHostsFile={cfg.known_hosts}",
                "-o", "StrictHostKeyChecking=yes", "-o", "BatchMode=yes",
                f"{cfg.user}@{cfg.host}"]
        try:
            proc = subprocess.run(argv, check=False, capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired as e:
            raise SftpUploadError(f"sftp timed out after {timeout}s: {e!r}") from e
        except FileNotFoundError as e:
            raise SftpUploadError(f"sftp binary not found: {e!r}") from e
        if proc.returncode != 0:
            raise SftpUploadError(
                f"sftp exit={proc.returncode} stderr={proc.stderr.decode('utf-8', 'replace')!r}")


def upload(cfg: SftpConfig, plans: list[UploadPlan], *, retry_cfg: RetryConfig,
           timeout: float = 300.0, sleeper: Callable[[float], None] | None = None) -> None:
    retry_call(_run_sftp, cfg, _build_batch(plans), timeout=timeout,
               on=(SftpUploadError,), attempts=retry_cfg.max_attempts,
               base=retry_cfg.backoff_base, cap=retry_cfg.backoff_cap, jitter=retry_cfg.jitter,
               sleeper=sleeper if sleeper is not None else time.sleep)
```

🐍 **Python deep-dive — `subprocess`, safely:**
- **`subprocess.run(argv_list, ...)` with no `shell=True`.** Passing a *list* means the OS executes
  the binary directly with those exact arguments — *no shell parses them*, so a filename containing
  `; rm -rf /` is just a literal argument. Building a command *string* + `shell=True` is the classic
  injection vulnerability; the list form sidesteps it entirely.
- **`capture_output=True`** captures stdout/stderr (so we can put stderr in the error);
  **`check=False`** means we inspect `returncode` ourselves rather than have it raise;
  **`timeout=`** bounds the call so a stalled transfer can't wedge the daemon.
- **`shlex.quote`** safely quotes paths with spaces *inside the batch file*.
- **`tempfile.NamedTemporaryFile(delete=True)`** as a context manager auto-deletes the batch file on
  exit, even on error.

🔍 **SSH/security concept — `StrictHostKeyChecking=yes`.** SSH verifies the server's *host key* against
a known value to defeat man-in-the-middle attacks. The "convenient" `=no` disables that and will
upload your data to *whoever answers* — the most common SFTP security mistake. We pin `=yes` against an
operator-supplied `UserKnownHostsFile`, use `BatchMode=yes` (never prompt — fail fast instead of
hanging), and key-based auth (`-i`).

🧪 **Tests — `tests/test_sftp_uploader.py`** (monkeypatch the subprocess; never open a socket):

```python
import subprocess
from unittest.mock import MagicMock
from etl.config import RetryConfig, SftpConfig
from etl.sftp_uploader import UploadPlan, upload
from pathlib import Path

CFG = lambda p: SftpConfig("h", 2222, "etl", p/"k", "/incoming", p/"kh")
FAST = RetryConfig(5, 0.0, 0.0, 0.0)

def test_argv_enforces_strict_host_key(monkeypatch, tmp_path):
    seen = {}
    def fake_run(argv, **kw): seen["argv"] = argv; return MagicMock(returncode=0, stderr=b"")
    monkeypatch.setattr(subprocess, "run", fake_run)
    upload(CFG(tmp_path), [UploadPlan(tmp_path/"f.csv", "/r/f.csv")],
           retry_cfg=FAST, sleeper=lambda _: None)
    assert "StrictHostKeyChecking=yes" in seen["argv"] and "BatchMode=yes" in seen["argv"]

def test_retries_then_succeeds(monkeypatch, tmp_path):
    calls = {"n": 0}
    def fake_run(argv, **kw):
        calls["n"] += 1
        return MagicMock(returncode=0 if calls["n"] >= 3 else 1, stderr=b"x")
    monkeypatch.setattr(subprocess, "run", fake_run)
    upload(CFG(tmp_path), [UploadPlan(tmp_path/"f", "/r/f")], retry_cfg=FAST, sleeper=lambda _: None)
    assert calls["n"] == 3
```

✅ **Definition of Done:** SFTP tests green (argv asserts strict host checking; retry behavior
verified) — *without any SFTP server*. 🏁 **PHASE D MILESTONE:** validation and delivery are complete
and fully unit-tested offline.

---

# PHASE E — Control plane & wiring (Epic: ETL-E)

## 🎫 ETL-E1 — Kafka control consumer  *(est. 1 d)*

🎯 **Goal:** consume the control topic, decode each message into a `ControlMessage`, and hand the
caller a manual-commit callback — with poison/null handling.

📨 **Kafka concept — offsets & commits.** A partition is an ordered log; each message has a monotonic
**offset**. A consumer reads forward and periodically *commits* how far it got (per `group.id`).
Commits are **cumulative**: committing offset N means "everything ≤ N is done." We disabled
auto-commit (ETL-A3), so *we* commit — only after a job fully succeeds (at-least-once delivery).

📄 **`src/etl/control_consumer.py`:**

```python
from __future__ import annotations

import json
import logging
from collections.abc import Callable, Iterator
from typing import Any

from etl.config import KafkaConfig
from etl.errors import ControlMessageError
from etl.models import ControlMessage

_log = logging.getLogger(__name__)


class ControlConsumer:
    def __init__(self, cfg: KafkaConfig, *,
                 consumer_factory: Callable[[dict[str, str]], Any] | None = None) -> None:
        if consumer_factory is None:
            from confluent_kafka import Consumer       # local import: heavy native dep
            consumer_factory = Consumer
        self._cfg = cfg
        self._consumer = consumer_factory(cfg.confluent_config())
        self._consumer.subscribe([cfg.control_topic])

    @staticmethod
    def _decode(raw: bytes, *, partition: int, offset: int) -> ControlMessage:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ControlMessageError(f"undecodable control message: {e!r}") from e
        if not isinstance(payload, dict):
            raise ControlMessageError("control message must be a JSON object")
        job_doc_id = payload.get("job_doc_id") or payload.get("id")
        if not isinstance(job_doc_id, str) or not job_doc_id:
            raise ControlMessageError("control message missing required 'job_doc_id'")
        correlation_id = payload.get("correlation_id")
        if correlation_id is not None and not isinstance(correlation_id, str):
            raise ControlMessageError("'correlation_id' must be string or absent")
        return ControlMessage(job_doc_id=job_doc_id, correlation_id=correlation_id,
                              raw_partition=partition, raw_offset=offset)

    def iter_messages(self, *, poll_timeout_s: float = 1.0,
                      stop: Callable[[], bool] | None = None
                      ) -> Iterator[tuple[ControlMessage, Callable[[], None], Any]]:
        while True:
            if stop is not None and stop():
                return
            msg = self._consumer.poll(timeout=poll_timeout_s)
            if msg is None:
                continue
            if msg.error():
                _log.warning("kafka poll error: %r", msg.error()); continue
            partition = int(msg.partition() or 0)
            offset = int(msg.offset() or 0)
            value = msg.value()
            if value is None:                          # tombstone: nothing to do
                self._consumer.commit(message=msg, asynchronous=False); continue
            raw = value.encode("utf-8") if isinstance(value, str) else value
            try:
                ctrl = self._decode(raw, partition=partition, offset=offset)
            except ControlMessageError as e:           # poison: can never succeed -> skip past
                _log.error("poison message p=%s o=%s: %r", partition, offset, e)
                self._consumer.commit(message=msg, asynchronous=False); continue

            def _commit(_m: Any = msg) -> None:
                self._consumer.commit(message=_m, asynchronous=False)

            yield ctrl, _commit, msg

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception as e:
            _log.warning("consumer close failed: %r", e)
```

🐍 **Python deep-dive:**
- **Dependency injection via `consumer_factory`.** Production uses the real `confluent_kafka.Consumer`;
  tests pass a `FakeConsumer`. The class only needs "something constructible from a config dict with
  `poll/commit/subscribe/close`." This is the seam that lets you test a Kafka consumer with no broker.
- **The closure default-argument trick.** `def _commit(_m=msg):` *captures the current `msg`* at
  definition time. Without `_m=msg`, the closure would capture the loop *variable* and every commit
  would refer to the last message — the classic Python late-binding bug.
- **Generator yielding a tuple of `(data, action, raw)`.** The consumer yields the *commit action*
  rather than committing itself — pushing the "when to acknowledge" decision up to the orchestrator,
  which knows whether the whole job succeeded.
- **Poison vs. transient.** A message that can *never* decode is committed-past (skipped) so it can't
  wedge the consumer forever. (A failing *valid* job is handled very differently — ETL-E3.)

🧪 **Tests — `tests/test_control_consumer.py`:**

```python
import json
from etl.config import KafkaConfig
from etl.control_consumer import ControlConsumer

class FakeConsumer:
    def __init__(self, cfg): self.cfg = cfg; self.committed = []; self.queue = []
    def subscribe(self, topics): self.subscribed = topics
    def poll(self, timeout): return self.queue.pop(0) if self.queue else None
    def commit(self, *, message, asynchronous): self.committed.append(message)
    def close(self): ...

class FakeMsg:
    def __init__(self, v, p=0, o=0): self._v, self._p, self._o = v, p, o
    def value(self): return self._v
    def partition(self): return self._p
    def offset(self): return self._o
    def error(self): return None

def test_decode_and_commit_only_after_ack():
    held = {}
    cc = ControlConsumer(KafkaConfig("b", "ctl", "g"),
                         consumer_factory=lambda c: held.setdefault("c", FakeConsumer(c)))
    held["c"].queue.append(FakeMsg(json.dumps({"job_doc_id": "abc"}).encode(), p=2, o=9))
    ctrl, commit, _ = next(cc.iter_messages(poll_timeout_s=0.0, stop=lambda: not held["c"].queue))
    assert ctrl.job_doc_id == "abc" and ctrl.raw_offset == 9
    assert held["c"].committed == []      # not committed yet
    commit()
    assert len(held["c"].committed) == 1  # committed only after ack
```

✅ **Definition of Done:** consumer tests green — decode, commit-only-after-ack, poison/null skip;
mypy clean (note `confluent_kafka` ships `py.typed`, so `value()`/`partition()` are `Optional` —
hence the `or 0`/None handling).

---

## 🎫 ETL-E2 — Pipeline orchestration  *(est. 0.5–1 d)*

🎯 **Goal:** compose all stages into `run_one`, the function that processes a single control message
(without committing — that's the daemon's call).

📄 **`src/etl/pipeline.py`** (core):

```python
def run_one(*, ctrl: ControlMessage, es: Any, settings: Settings) -> None:
    log_extra = {"job_doc_id": ctrl.job_doc_id, "correlation_id": ctrl.correlation_id,
                 "kafka_partition": ctrl.raw_partition, "kafka_offset": ctrl.raw_offset}
    job = load_job(es, job_index=settings.es.job_index, job_doc_id=ctrl.job_doc_id)
    log_extra["job_id"] = job.job_id

    initial_count = expected_count(es, job.data_index, job.query)
    _log.info("expected_count=%d", initial_count, extra=log_extra)

    strategy = make_strategy(
        settings.pagination.strategy,
        keep_alive=settings.pagination.scroll_keep_alive
        if settings.pagination.strategy == "scroll"
        else settings.pagination.pit_keep_alive,
    )
    local_csv, remote_csv, remote_sidecar = _staged_paths(settings.csv_output_dir, job)
    csv_result = _do_extract_to_csv(es=es, job=job, strategy=strategy,
                                    page_size=settings.pagination.page_size, local_csv=local_csv)

    def _reextract() -> CsvResult:                    # closure handed to the validator
        s2 = make_strategy(
            settings.pagination.strategy,
            keep_alive=settings.pagination.scroll_keep_alive
            if settings.pagination.strategy == "scroll"
            else settings.pagination.pit_keep_alive,
        )
        return _do_extract_to_csv(es=es, job=job, strategy=s2,
                                  page_size=settings.pagination.page_size, local_csv=local_csv)

    csv_result = validate_with_retry(es=es, index=job.data_index, query=job.query,
                                     csv_result=csv_result, retry_cfg=settings.retry,
                                     on_full_reextract=_reextract, log_extra=log_extra)
    upload(settings.sftp,
           [UploadPlan(local=csv_result.csv_path, remote=remote_csv),
            UploadPlan(local=csv_result.sidecar_path, remote=remote_sidecar)],
           retry_cfg=settings.retry)
```

`_do_extract_to_csv` is the reusable chain: `iter_hits → iter_transformed → write_csv`. `_staged_paths`
derives the local filename from the remote one's basename.

🐍 **Python deep-dive — composition + closures.** `run_one` is *composition*: each stage is a function;
`run_one` wires their inputs/outputs. The `_reextract` *closure* captures `es`, `job`, `settings`,
`local_csv` and hands the validator a zero-argument callable that knows how to rebuild the CSV
(rebuilding a *fresh* strategy, since the first one's scroll/PIT is spent). Note `run_one` takes `es`
and `settings` as *parameters* (dependency injection) — that is exactly why it's testable end-to-end
with fakes.

🧪 **Tests — `tests/test_pipeline.py`** (the golden path, all fakes):

```python
def test_run_one_golden_path(monkeypatch, settings, ...):
    # fake es: get(job doc) -> JobSpec source; count() -> 2; search()/scroll() -> 2 hits
    # monkeypatch subprocess.run -> returncode 0
    run_one(ctrl=ControlMessage("j1", None, 0, 0), es=fake_es, settings=settings)
    # assert CSV file exists with 2 data rows, and sftp was invoked
```

✅ **Definition of Done:** `test_pipeline.py` golden-path test green — a fake control message produces
a CSV and triggers a (faked) SFTP upload, with no real infrastructure.

---

## 🎫 ETL-E3 — Daemon entry point  *(est. 1 d)*

🎯 **Goal:** the long-lived process: poll, run a job, commit on success, **halt on failure**, shut down
gracefully on signals.

📄 **`src/etl/__main__.py`** (the loop):

```python
def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as e:
        print(f"config error: {e}", file=sys.stderr); return 2
    configure_logging(settings.log_level)

    stopping = {"flag": False}
    def _on_signal(signum, _frame): stopping["flag"] = True
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    es = _build_es_client(settings)
    consumer = ControlConsumer(settings.kafka)
    exit_code = 0
    try:
        for ctrl, commit, _raw in consumer.iter_messages(stop=lambda: stopping["flag"]):
            try:
                run_one(ctrl=ctrl, es=es, settings=settings)
            except EtlError as e:
                _log.error("job failed; halting without commit so it is redelivered: %r", e,
                           extra={"job_doc_id": ctrl.job_doc_id})
                exit_code = 1
                break                       # <-- NOT continue
            commit()
    finally:
        consumer.close()
        with contextlib.suppress(Exception):
            es.close()
    return exit_code
```

📨 **Kafka concept — why `break`, not `continue` (the subtle bug).** Commits are cumulative. If
message 5 fails and you `continue` (no commit), then message 6 succeeds and you commit 6 — committing
6 marks 5 done too, **silently losing the failed job**. So on a hard failure we *halt without
committing*: the offset stays put and on restart the message is redelivered. (By the time `EtlError`
reaches here, the pipeline already exhausted its internal retries, so this is a genuine failure worth
stopping and alerting on.)

🐍 **Python deep-dive:**
- **Signal handlers set a flag; the loop checks it.** We never abort mid-job — we finish the current
  job, then `stop()` ends the loop. Graceful shutdown = no half-written, half-uploaded jobs.
- **`contextlib.suppress(Exception)`** is a clean "best-effort" context manager for cleanup that
  shouldn't mask the real outcome (closing the ES client).
- **Distinct exit codes** (`2` config error, `1` job failure, `0` clean) let an orchestrator react.

🧪 **Tests:** the loop is integration-shaped; cover it with the Phase F smoke test rather than a unit
test. (Keep `run_one` and `ControlConsumer` independently unit-tested — they hold the logic.)

✅ **Definition of Done:** `python -m etl` starts, logs "starting", and blocks on poll. 🏁 **PHASE E
MILESTONE:** the daemon is wired end-to-end. With the local stack (Phase F) it processes a real
control message into a delivered file.

---

# PHASE F — Local stack & smoke test (Epic: ETL-F)

## 🎫 ETL-F1 — docker-compose + scripts + seed  *(est. 1–1.5 d)*

🎯 **Goal:** a one-command local environment (Kafka + ES + SFTP) and a seed script, so you can run the
real daemon against disposable infrastructure and execute the failure-mode drills.

📄 Provide `docker-compose.yml` (Kafka in KRaft mode, ES single-node with security off, `atmoz/sftp`),
`scripts/setup_local.sh` (generates SSH keys + host keys + captures `known_hosts`, creates the control
topic), `scripts/teardown_local.sh`, and `scripts/seed.py` (writes a job doc + sample data + one
control message). See the repo for the full files — they're plumbing, not application logic.

🔍 **Infra concept — why these shapes.** KRaft mode runs Kafka without ZooKeeper (simpler local
cluster). ES "security off" is fine for a *local mock* only. `atmoz/sftp` gives a key-only SFTP server
so the strict-host-key path (ETL-D2) has a real host key to verify against — `setup_local.sh`
`ssh-keyscan`s it into `known_hosts`.

🧪 **The smoke test (manual, end-to-end):**

```bash
./scripts/setup_local.sh
cp .env.local .env
.venv/bin/python scripts/seed.py
.venv/bin/python -m etl        # processes the control message
ls local/sftp/upload/          # CSV + .sha256 delivered
sha256sum -c local/sftp/upload/*.sha256
```

**Failure-mode drills (prove the safety nets):**
- *Count mismatch:* delete a doc from ES before starting the daemon → 5 count retries + 1 re-extract →
  `RecordCountMismatch`; offset not committed.
- *SFTP down:* `docker stop etl-sftp` → 5 retries with backoff → `SftpUploadError`; daemon halts.
- *Redelivery:* Ctrl-C mid-job, restart → same message reprocessed.

✅ **Definition of Done / 🏁 PROJECT MILESTONE:** the smoke test delivers a CSV whose checksum verifies,
and all three failure drills behave as described. **The service is complete and demonstrably correct
end-to-end.**

---

# PHASE G — Hardening (Epic: ETL-G, optional/backlog)

These turn a correct service into an operable product. Each is its own ticket:

- **ETL-G1 — CI pipeline:** run `ruff` + `mypy` + `pytest` (with a coverage gate) on every PR.
- **ETL-G2 — ES 9.x readiness:** replace the deprecated `body={"query": ...}` calls with explicit
  kwargs (`es.count(index=..., query=...)`, `es.search(index=..., query=..., size=..., sort=...,
  pit=...)`) and lift the `<9` pin.
- **ETL-G3 — Dead-letter & alerting:** route failed/poison jobs to a DLQ topic with context instead of
  bare halt/skip; emit an alert.
- **ETL-G4 — Production security:** ES TLS/CA verification, secrets from a vault (not flat `.env`),
  key-file permission checks.
- **ETL-G5 — Observability:** per-job metrics + OpenTelemetry spans across stages.
- **ETL-G6 — Packaging & deploy:** container image + Helm/compose with liveness/readiness.

---

# Appendix A — Jira ticket summary

| Ticket | Title | Phase/Epic | Est. | Milestone when done |
|---|---|---|---|---|
| ETL-A1 | Scaffolding & tooling | A Foundations | 0.5 d | harness green |
| ETL-A2 | Errors & models | A | 0.5 d | vocabulary importable |
| ETL-A3 | Configuration | A | 0.5–1 d | config validated |
| ETL-A4 | Structured logging | A | 0.5 d | JSON logs |
| ETL-A5 | Retry primitive | A | 1 d | **Phase A green** |
| ETL-B1 | Transformer | B Transform core | 1 d | projection tested |
| ETL-B2 | CSV writer + sidecar | B | 0.5–1 d | **Stage 0: offline CSV** |
| ETL-C1 | Pagination + extractor | C Elasticsearch | 1.5–2 d | streaming + cleanup tested |
| ETL-C2 | Job loader | C | 0.5 d | **live extraction** |
| ETL-D1 | Validator | D Trust & delivery | 1–1.5 d | count safety tested |
| ETL-D2 | SFTP uploader | D | 1 d | **delivery tested offline** |
| ETL-E1 | Control consumer | E Wiring | 1 d | consume + commit tested |
| ETL-E2 | Pipeline | E | 0.5–1 d | golden path tested |
| ETL-E3 | Daemon entry point | E | 1 d | **daemon runs** |
| ETL-F1 | Local stack + smoke | F Smoke | 1–1.5 d | **end-to-end green** |
| ETL-G1–G6 | Hardening | G | ongoing | productionized |

**Core build total (A–F): ≈ 12–17 engineer-days.** Each ticket ends on a green test command, so
progress is always verifiable and the branch is always shippable.

# Appendix B — Concept index

- **Python:** `from __future__ import annotations` (A2) · frozen dataclasses (A2) · exception chaining
  `raise ... from e` (A3) · structured logging via `record.__dict__` (A4) · `TypeVar`/generics,
  decorators, injectable clock, `except tuple` (A5) · generators / `Iterable` vs `Iterator` (B1) ·
  duck typing + decorator-wrapper, context managers (B2) · `typing.Protocol` structural typing,
  resource-owning generators with `finally` (C1) · generator-expr validation (C2) · inversion of
  control via callback (D1) · `subprocess` without `shell=True`, `shlex.quote`, `tempfile` (D2) ·
  closure default-arg capture, DI factory (E1) · composition + closures (E2) · signals,
  `contextlib.suppress` (E3).
- **Kafka:** consumer config keys (A3) · offsets, cumulative commits, at-least-once (E1) ·
  halt-vs-continue offset-loss (E3) · KRaft local mode (F1).
- **Elasticsearch:** deep-pagination limits, Scroll (`scroll`, `size`, `_scroll_id`, `clear_scroll`),
  PIT + `search_after` (`keep_alive`, `_shard_doc`, `track_total_hits`) (C1) · `es.get` by id (C2) ·
  response shape `hits.hits[]._source` (C1) · near-real-time refresh races (D1).
```
