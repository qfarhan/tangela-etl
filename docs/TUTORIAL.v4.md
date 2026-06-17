# Test-Driven Build Tutorial — v4 (Checkpoint by Checkpoint)

**What this is.** A build guide you can *follow with your hands on the keyboard*. You build the
`kafka-es-csv-sftp-etl` service **one module at a time**, and after every module you write that
module's test, run **one command**, and watch it go green **before you move on**. Each module is a
**checkpoint**: a verified, locked-in piece of working software. You never build on sand.

**How this differs from the other docs:**

| Doc | Answers | Shape |
|---|---|---|
| [`DESIGN.md`](DESIGN.md) | *Why* each decision was made | theory & patterns |
| [`TUTORIAL.v3.md`](TUTORIAL.v3.md) | *What every line does* | line-by-line reading of finished code |
| **this (`TUTORIAL.v4.md`)** | *How to build it incrementally, proving each step* | red→green, checkpoint by checkpoint |

The earlier build logs ([`TUTORIAL.md`](TUTORIAL.md) / [`TUTORIAL.v2.md`](TUTORIAL.v2.md)) sketched the
phases but used *abridged toy tests*. **This version embeds the real test files from `tests/`** — the
exact code that ships — so the command you run at each checkpoint is the command CI runs. When it's
green for you, the functionality is genuinely there.

**The one discipline that makes this work:** *never start the next checkpoint until the current one is
green.* A growing codebase only stays sane if every layer rests on a **verified** layer beneath it.
The single exception is the very last phase, where we deliberately introduce a *red* bar — see
[Phase F](#phase-f--the-full-suite--the-intentional-red-bar).

---

## The checkpoint ledger

Every row is a checkpoint. Build the module, paste the test, run the command, confirm the count, tick
the box. The **Cumulative** column is what the *whole* suite-so-far reports — a running proof that
nothing you built earlier regressed.

| ✅ | Checkpoint | Module you build | Test you run | Tests | Cumulative |
|---|---|---|---|---|---|
| ☐ | **A1** Scaffolding | `pyproject.toml`, `src/` layout | `pytest` (collects nothing) | 0 | 0 |
| ☐ | **A2** Vocabulary | `etl/errors.py`, `etl/models.py` | `pytest tests/test_models.py` | 2 | 2 |
| ☐ | **A3** Config + harness | `etl/config.py`, `tests/conftest.py` | `pytest tests/test_config.py` | 4 | 6 |
| ☐ | **A4** Logging | `etl/logging_setup.py` | `pytest tests/test_logging.py` | 1 | 7 |
| ☐ | **A5** Retry | `etl/retry.py` | `pytest tests/test_retry.py` | 8 | 15 |
| ☐ | **B1** Transformer | `etl/transformer.py` | `pytest tests/test_transformer.py` | 9 | 24 |
| ☐ | **B2** CSV + sidecar | `etl/csv_writer.py` | `pytest tests/test_csv_writer.py` | 5 | 29 |
| ☐ | **C1** ES extractor (pkg) | `es_extract/*` | `pytest tests/test_es_extract.py` | 11 | 40 |
| ☐ | **C2** ES bridge | `etl/extractor.py` | `pytest tests/test_extractor.py` | 4 | 44 |
| ☐ | **C3** Job loader | `etl/job_loader.py` | `pytest tests/test_job_loader.py` | 7 | 51 |
| ☐ | **D1** Validator | `etl/validator.py` | `pytest tests/test_validator.py` | 6 | 57 |
| ☐ | **D2** SFTP uploader | `etl/sftp_uploader.py` | `pytest tests/test_sftp_uploader.py` | 6 | 63 |
| ☐ | **E1** Control consumer | `etl/control_consumer.py` | `pytest tests/test_control_consumer.py` | 4 | 67 |
| ☐ | **E2** Pipeline | `etl/pipeline.py` | `pytest tests/test_pipeline.py` | 5 | 72 |
| ☐ | **E3** Daemon | `etl/__main__.py` | `python -m etl` (smoke) | 0 | 72 |
| ☐ | **F** Full suite + adversarial | `tests/test_adversarial.py` | `pytest` | 75✓ / 9✗ | 75 pass |
| ☐ | **G** Local stack & e2e | compose + scripts | manual smoke + drills | — | delivered |

**Final state:** `pytest` reports **75 passed, 9 failed** — and the 9 failures are *intentional*
(documentation-as-tests for known gaps; Phase F explains them and how to reach an all-green bar).

---

## How each checkpoint is laid out

```
🎯 Goal        — what this module adds, and why now
📦 Depends on  — which earlier checkpoints must already be green
📄 Build       — the full module source. Type it (or paste it); it is byte-accurate to the repo.
🧪 Test        — the full, real test file for this module
▶️  Run         — the exact command
✅ Checkpoint  — the output you must see before continuing (+ the cumulative whole-suite count)
💡 Why         — the key technique(s) and what the test actually proves
```

Every Python module starts with `from __future__ import annotations` (annotations become lazy strings:
forward references work, import cost drops). The whole project is `mypy --strict` clean, so run
`mypy src` whenever you like a second opinion — but the **green test** is the gate that lets you move on.

### One-time workspace setup (before A1)

```bash
mkdir -p kafka-es-csv-sftp-etl/src/etl kafka-es-csv-sftp-etl/src/es_extract kafka-es-csv-sftp-etl/tests
cd kafka-es-csv-sftp-etl
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
```

Commands below assume your venv is active (or prefix with `.venv/bin/`). Python **3.10+** required.

---

# PHASE A — Foundations

Five checkpoints: a buildable, type-checked skeleton plus the cross-cutting primitives (errors, config,
logging, retry) that *every* later module imports. No Kafka or ES yet — but you finish Phase A with
**15 green tests** and a foundation you can trust.

## ✅ A1 — Scaffolding & tooling

🎯 **Goal:** an installable package with pytest/ruff/mypy wired up, so from now on every checkpoint can
end on a green `pytest`.

📦 **Depends on:** nothing.

📄 **Build — `pyproject.toml`** (repo root):

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "kafka-es-csv-sftp-etl"
version = "0.1.0"
description = "Control-driven ETL: Kafka control topic -> Elasticsearch -> dotted-path projection -> CSV -> SFTP"
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
addopts = "-ra -q"
pythonpath = ["src"]

[tool.ruff]
line-length = 100
target-version = "py310"
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "B", "UP", "SIM", "RUF"]
ignore = ["E501"]

[tool.mypy]
python_version = "3.10"
strict = true
packages = ["etl", "es_extract"]
mypy_path = "src"
```

Create the two package markers and install:

```bash
printf '__version__ = "0.1.0"\n' > src/etl/__init__.py
touch src/es_extract/__init__.py        # we flesh this out at C1
.venv/bin/pip install -e ".[dev]"
```

▶️ **Run:**

```bash
ruff check src tests
pytest
```

✅ **Checkpoint:** `ruff` prints *All checks passed!* and `pytest` exits cleanly with **no tests ran /
collected 0 items**. That "nothing to run, but the harness works" state is success here.
**Cumulative: 0.**

💡 **Why.** The `src/` layout means the *only* way to import `etl` is to install it — killing the
"works because Python imported the folder" class of bug. `pythonpath = ["src"]` lets pytest import the
packages without re-installing after every edit. `[project.scripts]` makes both `etl` and `python -m etl`
real entry points. You now have a green harness to build into.

---

## ✅ A2 — The vocabulary: errors & models

🎯 **Goal:** define the typed data and the exception hierarchy the whole pipeline speaks in. Pure
Python, zero dependencies — the ideal first *real* code, and the first test you can turn green.

📦 **Depends on:** A1.

📄 **Build — `src/etl/errors.py`:**

```python
"""Exception hierarchy for the ETL pipeline.

All recoverable, job-scoped failures inherit from `EtlError` so the consumer
loop can catch them at the boundary and keep running. Anything not derived
from `EtlError` (e.g., import errors, programmer bugs) is allowed to crash
the process.
"""

from __future__ import annotations


class EtlError(Exception):
    """Base class for all ETL-pipeline errors."""


class ConfigError(EtlError):
    """Raised when required configuration is missing or invalid."""


class ControlMessageError(EtlError):
    """Raised when a Kafka control message can't be decoded or is missing fields."""


class JobSpecError(EtlError):
    """Raised when the ES-resident job document is missing or malformed."""


class ElasticsearchQueryError(EtlError):
    """Raised on ES request failures (count, search, PIT)."""


class TransformError(EtlError):
    """Raised when applying the JOLT spec to an ES hit fails.

    Carries the job_id, hit_id (if any), and the offending JOLT operation
    name so the failure can be diagnosed without re-reading the full doc.
    """

    def __init__(self, message: str, *, job_id: str, hit_id: str | None = None,
                 jolt_op: str | None = None) -> None:
        super().__init__(message)
        self.job_id = job_id
        self.hit_id = hit_id
        self.jolt_op = jolt_op


class CsvWriteError(EtlError):
    """Raised on local CSV / sidecar write failures."""


class RecordCountMismatch(EtlError):
    """Raised when ES `_count` and CSV row count disagree after all retries."""

    def __init__(self, expected: int, actual: int, attempts: list[tuple[int, int]]) -> None:
        super().__init__(
            f"record count mismatch: expected={expected} actual={actual} "
            f"attempts={attempts}"
        )
        self.expected = expected
        self.actual = actual
        self.attempts = attempts


class SftpUploadError(EtlError):
    """Raised when the sftp subprocess exits non-zero or times out."""


class RetryExhausted(EtlError):
    """Wraps the final exception after retry attempts are exhausted.

    Only raised when the retry decorator is invoked with wrap_final=True.
    The default behaviour is to re-raise the original exception unchanged.
    """

    def __init__(self, attempts: int, last_exc: BaseException) -> None:
        super().__init__(f"retry exhausted after {attempts} attempts: {last_exc!r}")
        self.attempts = attempts
        self.last_exc = last_exc
```

> 📝 *Known staleness, kept on purpose:* `TransformError`'s docstring and its `jolt_op` field still
> reference JOLT, which the project dropped for dotted-path projection (B1). Nothing sets/reads
> `jolt_op` — it's dead. We keep it so the code matches the rest of the docs; it's flagged for cleanup
> (`REVIEW.v3.md` §3.2). Building the *real* code means inheriting its real warts.

📄 **Build — `src/etl/models.py`:**

```python
"""Plain-data types passed between pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ControlMessage:
    """A decoded message from the Kafka control topic.

    `raw_partition` and `raw_offset` are kept so the orchestrator can commit
    the exact offset back to Kafka after a successful job.
    """

    job_doc_id: str
    correlation_id: str | None
    raw_partition: int
    raw_offset: int


@dataclass(frozen=True)
class JobSpec:
    """Describes a single export job, loaded by id from the ES job-index.

    `query` is the ES query body (DSL). `column_paths` maps each CSV column
    name to a dotted path into the source document (e.g. ``user.id``);
    columns absent from the mapping fall back to a same-name top-level
    lookup. `columns` controls CSV column order.
    """

    job_id: str
    data_index: str
    query: dict[str, Any]
    column_paths: dict[str, str]
    columns: list[str]
    remote_filename: str


@dataclass(frozen=True)
class CsvResult:
    """Result of streaming an iterator of rows to disk."""

    csv_path: Path
    sidecar_path: Path
    row_count: int
    sha256_hex: str
```

🧪 **Test — `tests/test_models.py`:**

```python
from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from etl.errors import RecordCountMismatch
from etl.models import JobSpec


def test_jobspec_is_immutable() -> None:
    job = JobSpec("j", "idx", {"match_all": {}}, {}, ["a"], "out.csv")
    with pytest.raises(FrozenInstanceError):
        job.data_index = "other"  # frozen dataclass forbids reassignment


def test_record_count_mismatch_carries_context() -> None:
    err = RecordCountMismatch(expected=5, actual=4, attempts=[(5, 4)])
    assert err.expected == 5
    assert err.actual == 4
    assert "expected=5" in str(err)
```

▶️ **Run:** `pytest tests/test_models.py`

✅ **Checkpoint:** `2 passed`. Whole suite so far: `pytest` → **2 passed. Cumulative: 2.**

💡 **Why.** A single base exception (`EtlError`) is a *classification tool*: later one `except EtlError`
catches every *expected* failure at the daemon boundary while a real bug (a `KeyError`) crashes loudly.
The two tests prove the two things the rest of the code relies on: frozen dataclasses really are
immutable (data can't drift between stages — `test_jobspec_is_immutable`), and the rich errors carry
their structured context into both attributes *and* the message (`test_record_count_mismatch_carries_context`).

---

## ✅ A3 — Configuration + the shared test harness

🎯 **Goal:** read every setting from the environment once, validate it, freeze it — and stand up the
**shared pytest fixtures** that almost every later test reuses.

📦 **Depends on:** A2 (`ConfigError`, and `JobSpec` for the harness).

📄 **Build — `src/etl/config.py`:**

```python
"""Environment-driven configuration.

All settings are loaded once at process start. Missing required values raise
`ConfigError` immediately — the daemon refuses to start with a bad config.
"""

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


def _get_float(key: str, default: float) -> float:
    raw = os.environ.get(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise ConfigError(f"{key} must be a float, got: {raw!r}") from e


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
        if self.security_protocol:
            cfg["security.protocol"] = self.security_protocol
        if self.sasl_mechanism:
            cfg["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            cfg["sasl.username"] = self.sasl_username
        if self.sasl_password:
            cfg["sasl.password"] = self.sasl_password
        return cfg


@dataclass(frozen=True)
class EsConfig:
    hosts: list[str]
    username: str | None
    password: str | None
    api_key: str | None
    job_index: str


@dataclass(frozen=True)
class PaginationConfig:
    page_size: int
    pit_keep_alive: str


@dataclass(frozen=True)
class RetryConfig:
    max_attempts: int = 5
    backoff_base: float = 1.0
    backoff_cap: float = 30.0
    jitter: float = 0.25


@dataclass(frozen=True)
class SftpConfig:
    host: str
    port: int
    user: str
    key_path: Path
    remote_dir: str
    known_hosts: Path


@dataclass(frozen=True)
class Settings:
    kafka: KafkaConfig
    es: EsConfig
    pagination: PaginationConfig
    retry: RetryConfig
    sftp: SftpConfig
    csv_output_dir: Path
    log_level: str = "INFO"
    # Optional diagnostic: when set, each job tees its raw extracted hits to
    # `<raw_dump_dir>/<job_id>.ndjson`. Unset (default) disables the dump.
    raw_dump_dir: Path | None = None


def load_settings(*, dotenv_path: str | None = None) -> Settings:
    """Load and validate settings from environment (and optional .env file)."""
    load_dotenv(dotenv_path=dotenv_path, override=False)

    kafka = KafkaConfig(
        bootstrap_servers=_get("KAFKA_BOOTSTRAP_SERVERS", required=True),
        control_topic=_get("KAFKA_CONTROL_TOPIC", required=True),
        group_id=_get("KAFKA_GROUP_ID", required=True),
        security_protocol=_get("KAFKA_SECURITY_PROTOCOL") or None,
        sasl_mechanism=_get("KAFKA_SASL_MECHANISM") or None,
        sasl_username=_get("KAFKA_SASL_USERNAME") or None,
        sasl_password=_get("KAFKA_SASL_PASSWORD") or None,
    )

    hosts_raw = _get("ES_HOSTS", required=True)
    es = EsConfig(
        hosts=[h.strip() for h in hosts_raw.split(",") if h.strip()],
        username=_get("ES_USERNAME") or None,
        password=_get("ES_PASSWORD") or None,
        api_key=_get("ES_API_KEY") or None,
        job_index=_get("ES_JOB_INDEX", required=True),
    )

    pagination = PaginationConfig(
        page_size=_get_int("PAGE_SIZE", 1000),
        pit_keep_alive=_get("PIT_KEEP_ALIVE", "5m"),
    )

    retry = RetryConfig(
        max_attempts=_get_int("RETRY_MAX_ATTEMPTS", 5),
        backoff_base=_get_float("RETRY_BACKOFF_BASE", 1.0),
        backoff_cap=_get_float("RETRY_BACKOFF_CAP", 30.0),
        jitter=_get_float("RETRY_JITTER", 0.25),
    )

    sftp = SftpConfig(
        host=_get("SFTP_HOST", required=True),
        port=_get_int("SFTP_PORT", 22),
        user=_get("SFTP_USER", required=True),
        key_path=Path(_get("SFTP_KEY_PATH", required=True)),
        # Optional: the job document's `remote_filename` is the authoritative
        # remote path. `remote_dir` is reserved for callers that want to build
        # remote paths from a base dir; it is not required to start the daemon.
        remote_dir=_get("SFTP_REMOTE_DIR"),
        known_hosts=Path(_get("SFTP_KNOWN_HOSTS", required=True)),
    )

    return Settings(
        kafka=kafka,
        es=es,
        pagination=pagination,
        retry=retry,
        sftp=sftp,
        csv_output_dir=Path(_get("CSV_OUTPUT_DIR", "/tmp/etl-csv")),
        log_level=_get("LOG_LEVEL", "INFO").upper(),
        raw_dump_dir=Path(raw_dump) if (raw_dump := _get("ES_RAW_DUMP_DIR")) else None,
    )
```

📄 **Build — `tests/conftest.py`** (the shared harness, set up *once*):

This is the most important non-shipping file in the build. A few fixtures here (`settings`,
`FakeKafkaMessage`) aren't exercised until Phases C–E; we define them now so the test harness lives in
one place. The standout is `_isolate_dotenv` — read its docstring.

```python
"""Shared pytest fixtures.

Note: every fixture here is a pure-Python fake. No real Kafka/ES/SFTP
clients are imported, so the test suite runs without network or Docker.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from etl.config import (
    EsConfig,
    KafkaConfig,
    PaginationConfig,
    RetryConfig,
    Settings,
    SftpConfig,
)
from etl.models import JobSpec


@pytest.fixture(autouse=True)
def _isolate_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep config tests hermetic.

    ``load_settings()`` calls ``dotenv.load_dotenv()``, which walks up from the
    current working directory and silently loads a developer's local ``.env``
    (e.g. the one created by ``cp .env.local .env`` during smoke testing). That
    repopulates vars a test deliberately removed and breaks isolation. Disable
    it for the whole suite so tests depend only on the env they set explicitly.
    """
    monkeypatch.setattr("etl.config.load_dotenv", lambda *a, **k: False)


@pytest.fixture
def retry_cfg_fast() -> RetryConfig:
    """Retry config with zero delays so unit tests don't sleep."""
    return RetryConfig(max_attempts=5, backoff_base=0.0, backoff_cap=0.0, jitter=0.0)


@pytest.fixture
def sample_job_spec() -> JobSpec:
    return JobSpec(
        job_id="job-1",
        data_index="my-data",
        query={"match_all": {}},
        column_paths={},
        columns=["id", "name", "value"],
        remote_filename="exports/job-1.csv",
    )


@pytest.fixture
def sample_hits() -> list[dict[str, Any]]:
    return [
        {"id": "a", "name": "alpha", "value": 1},
        {"id": "b", "name": "beta", "value": 2},
        {"id": "c", "name": "gamma", "value": 3},
    ]


@pytest.fixture
def tmp_csv_dir(tmp_path: Path) -> Path:
    d = tmp_path / "csv"
    d.mkdir()
    return d


@pytest.fixture
def settings(tmp_path: Path, tmp_csv_dir: Path, retry_cfg_fast: RetryConfig) -> Settings:
    return Settings(
        kafka=KafkaConfig(
            bootstrap_servers="localhost:9092",
            control_topic="ctl",
            group_id="g",
        ),
        es=EsConfig(
            hosts=["http://localhost:9200"],
            username=None,
            password=None,
            api_key=None,
            job_index="jobs",
        ),
        pagination=PaginationConfig(
            page_size=2,
            pit_keep_alive="1m",
        ),
        retry=retry_cfg_fast,
        sftp=SftpConfig(
            host="sftp.example.com",
            port=22,
            user="etl",
            key_path=tmp_path / "id",
            remote_dir="/incoming",
            known_hosts=tmp_path / "kh",
        ),
        csv_output_dir=tmp_csv_dir,
        log_level="DEBUG",
    )


class FakeMessage:
    def __init__(self, value: bytes, *, partition: int = 0, offset: int = 0,
                 err: Any = None) -> None:
        self._value = value
        self._partition = partition
        self._offset = offset
        self._err = err

    def value(self) -> bytes: return self._value
    def partition(self) -> int: return self._partition
    def offset(self) -> int: return self._offset
    def error(self) -> Any: return self._err


@pytest.fixture
def FakeKafkaMessage() -> type[FakeMessage]:
    return FakeMessage
```

> Note `settings` uses `page_size=2` on purpose — small enough that later multi-page pagination paths
> are actually exercised by tiny fixtures.

🧪 **Test — `tests/test_config.py`:**

```python
from __future__ import annotations

import pytest

from etl.config import load_settings
from etl.errors import ConfigError

_BASE_ENV: dict[str, str] = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_CONTROL_TOPIC": "ctl",
    "KAFKA_GROUP_ID": "g",
    "ES_HOSTS": "http://localhost:9200,http://other:9200",
    "ES_JOB_INDEX": "jobs",
    "SFTP_HOST": "h",
    "SFTP_USER": "u",
    "SFTP_KEY_PATH": "/tmp/key",
    "SFTP_REMOTE_DIR": "/r",
    "SFTP_KNOWN_HOSTS": "/tmp/kh",
}


def _set_env(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
    for key in list(env.keys()) + list(_BASE_ENV.keys()):
        monkeypatch.delenv(key, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)


def test_load_settings_with_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_env(monkeypatch, _BASE_ENV)
    s = load_settings()
    assert s.kafka.bootstrap_servers == "localhost:9092"
    assert s.es.hosts == ["http://localhost:9200", "http://other:9200"]
    assert s.pagination.page_size == 1000
    assert s.pagination.pit_keep_alive == "5m"
    assert s.retry.max_attempts == 5
    assert s.retry.backoff_base == 1.0
    assert s.sftp.port == 22


def test_load_settings_missing_required_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    env = dict(_BASE_ENV)
    env.pop("KAFKA_BOOTSTRAP_SERVERS")
    _set_env(monkeypatch, env)
    with pytest.raises(ConfigError, match="KAFKA_BOOTSTRAP_SERVERS"):
        load_settings()


def test_load_settings_retry_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    env = dict(
        _BASE_ENV,
        RETRY_MAX_ATTEMPTS="7",
        RETRY_BACKOFF_BASE="0.5",
        RETRY_BACKOFF_CAP="10",
        RETRY_JITTER="0.1",
    )
    _set_env(monkeypatch, env)
    s = load_settings()
    assert s.retry.max_attempts == 7
    assert s.retry.backoff_base == 0.5
    assert s.retry.backoff_cap == 10.0
    assert s.retry.jitter == 0.1


def test_load_settings_bad_integer(monkeypatch: pytest.MonkeyPatch) -> None:
    env = dict(_BASE_ENV, PAGE_SIZE="not-a-number")
    _set_env(monkeypatch, env)
    with pytest.raises(ConfigError, match="PAGE_SIZE"):
        load_settings()
```

▶️ **Run:** `pytest tests/test_config.py`

✅ **Checkpoint:** `4 passed`. Whole suite: `pytest` → **6 passed. Cumulative: 6.**

💡 **Why.** `_get(..., required=True)` turns "missing" into a precise `ConfigError` *at startup*, not a
confusing `None` deep in a loop; `_get_int` chains the parse failure with `raise ... from e` so
tracebacks keep the original `ValueError`. The headline lesson is **hidden inputs**: `load_dotenv()`
reads the *filesystem* — an ambient input your test never passed. A test that depends on the *absence*
of a file isn't hermetic. The `autouse` `_isolate_dotenv` neutralizes it for the whole suite. (This was
a real bug; you learn it here cheaply.) The four tests prove defaults apply, missing-required fails with
the offending var named, overrides flow through, and a non-integer is rejected.

> 🔍 **Kafka note (config keys you just hard-coded):** `enable.auto.commit=false` means *we* commit
> offsets manually after a job succeeds (the linchpin of at-least-once delivery, wired in E1/E3);
> `auto.offset.reset=earliest` means a brand-new consumer group starts at the beginning so no pending
> work is skipped.

---

## ✅ A4 — Structured logging

🎯 **Goal:** one JSON object per log line, with per-job context (`job_id`, offsets…) merged in — so logs
are both human-readable and machine-queryable.

📦 **Depends on:** A1.

📄 **Build — `src/etl/logging_setup.py`:**

```python
"""Minimal structured-JSON logging.

Each record is one JSON object per line. The `extra` dict on `logger.info(...)`
is merged into the payload so callers can attach `job_id`, `correlation_id`,
attempt numbers, etc. without juggling format strings.
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

_RESERVED = {
    "args", "asctime", "created", "exc_info", "exc_text", "filename",
    "funcName", "levelname", "levelno", "lineno", "message", "module",
    "msecs", "msg", "name", "pathname", "process", "processName",
    "relativeCreated", "stack_info", "thread", "threadName", "taskName",
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
                payload[key] = value
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

🧪 **Test — `tests/test_logging.py`:**

```python
from __future__ import annotations

import json
import logging

import pytest

from etl.logging_setup import configure_logging


def test_extra_fields_become_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO")
    logging.getLogger("t").info("hello", extra={"job_id": "abc"})
    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["msg"] == "hello"
    assert payload["job_id"] == "abc"
```

▶️ **Run:** `pytest tests/test_logging.py`

✅ **Checkpoint:** `1 passed`. Whole suite: `pytest` → **7 passed. Cumulative: 7.**

💡 **Why.** A `LogRecord` is just an object with a `__dict__`. When you call
`log.info("msg", extra={"job_id": "x"})`, Python sets `record.job_id = "x"`. The formatter walks
`record.__dict__`, skips the built-in *reserved* fields, and merges the rest — that's how arbitrary
context becomes queryable JSON. `json.dumps(..., default=str)` guarantees serialization never crashes
(a `Path`/`datetime` falls back to `str()`) and escapes newlines (defeating log-forging). The test
proves the contract directly: log with an `extra`, capture stdout, parse the last line, see both the
message and the merged field.

---

## ✅ A5 — The retry primitive

🎯 **Goal:** one reusable exponential-backoff-with-jitter helper that Kafka/ES/SFTP all build on — with
an **injectable clock** so tests spend *zero* real time. Richest "advanced Python" checkpoint in
Phase A.

📦 **Depends on:** A2 (`RetryExhausted`).

📄 **Build — `src/etl/retry.py`:**

```python
"""Generic exponential-backoff-with-jitter retry helper.

`retry_call` is the building block. `@retry(...)` is the decorator form.
The `sleeper` argument is exposed so tests can pass a fake clock instead of
calling `time.sleep` for real.
"""

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
    fn: Callable[..., T],
    *args: Any,
    on: tuple[type[BaseException], ...],
    attempts: int = 5,
    base: float = 1.0,
    cap: float = 30.0,
    jitter: float = 0.25,
    sleeper: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    wrap_final: bool = False,
    log_extra: dict[str, Any] | None = None,
    **kwargs: Any,
) -> T:
    """Call `fn` with retry-on-exception semantics.

    Re-raises the original exception after the final attempt unless
    `wrap_final=True`, in which case it raises `RetryExhausted` wrapping it.
    """
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
            _log.warning(
                "retry: %s attempt=%d/%d delay=%.3fs err=%r",
                getattr(fn, "__name__", repr(fn)),
                attempt + 1,
                attempts,
                delay,
                exc,
                extra={**(log_extra or {}), "retry_attempt": attempt + 1,
                       "retry_delay_s": delay},
            )
            sleeper(delay)
    assert last is not None
    if wrap_final:
        raise RetryExhausted(attempts, last) from last
    raise last


def retry(
    *,
    on: tuple[type[BaseException], ...],
    attempts: int = 5,
    base: float = 1.0,
    cap: float = 30.0,
    jitter: float = 0.25,
    sleeper: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    wrap_final: bool = False,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator form of `retry_call`."""

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return retry_call(
                fn,
                *args,
                on=on,
                attempts=attempts,
                base=base,
                cap=cap,
                jitter=jitter,
                sleeper=sleeper,
                rng=rng,
                wrap_final=wrap_final,
                **kwargs,
            )

        return wrapper

    return decorator
```

🧪 **Test — `tests/test_retry.py`:**

```python
from __future__ import annotations

import random

import pytest

from etl.errors import RetryExhausted
from etl.retry import retry, retry_call


class Boom(Exception):
    pass


def test_retry_succeeds_first_try_no_sleep() -> None:
    sleeps: list[float] = []
    result = retry_call(
        lambda: 42,
        on=(Boom,),
        attempts=5,
        sleeper=sleeps.append,
    )
    assert result == 42
    assert sleeps == []


def test_retry_succeeds_on_third_attempt_sleeps_twice() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise Boom(f"attempt {calls['n']}")
        return "ok"

    result = retry_call(
        flaky,
        on=(Boom,),
        attempts=5,
        base=1.0,
        cap=30.0,
        jitter=0.0,
        sleeper=sleeps.append,
    )
    assert result == "ok"
    assert sleeps == [1.0, 2.0]  # 1*2^0, 1*2^1, jitter disabled


def test_retry_exhausts_and_raises_original() -> None:
    sleeps: list[float] = []

    def always_fail() -> None:
        raise Boom("nope")

    with pytest.raises(Boom):
        retry_call(
            always_fail,
            on=(Boom,),
            attempts=3,
            base=1.0,
            cap=30.0,
            jitter=0.0,
            sleeper=sleeps.append,
        )
    # 3 attempts → 2 sleeps between them.
    assert sleeps == [1.0, 2.0]


def test_retry_wrap_final_emits_retry_exhausted() -> None:
    def always_fail() -> None:
        raise Boom("nope")

    with pytest.raises(RetryExhausted) as ei:
        retry_call(
            always_fail,
            on=(Boom,),
            attempts=2,
            base=0.0,
            cap=0.0,
            jitter=0.0,
            sleeper=lambda _: None,
            wrap_final=True,
        )
    assert ei.value.attempts == 2
    assert isinstance(ei.value.last_exc, Boom)


def test_retry_does_not_catch_unrelated_exceptions() -> None:
    sleeps: list[float] = []

    class Other(Exception):
        pass

    def raise_other() -> None:
        raise Other("not in catch list")

    with pytest.raises(Other):
        retry_call(
            raise_other, on=(Boom,), attempts=5, sleeper=sleeps.append,
        )
    assert sleeps == []


def test_retry_decorator_form() -> None:
    sleeps: list[float] = []
    calls = {"n": 0}

    @retry(on=(Boom,), attempts=3, base=1.0, cap=30.0, jitter=0.0,
           sleeper=sleeps.append)
    def f() -> str:
        calls["n"] += 1
        if calls["n"] < 2:
            raise Boom("once")
        return "done"

    assert f() == "done"
    assert sleeps == [1.0]


def test_retry_attempts_must_be_positive() -> None:
    with pytest.raises(ValueError):
        retry_call(lambda: None, on=(Boom,), attempts=0)


def test_retry_jitter_uses_injected_rng() -> None:
    # Seed rng so result is deterministic; verify jitter scales the delay
    # within the expected bound.
    sleeps: list[float] = []
    rng = random.Random(0)

    def always_fail() -> None:
        raise Boom("x")

    with pytest.raises(Boom):
        retry_call(
            always_fail, on=(Boom,), attempts=2,
            base=1.0, cap=30.0, jitter=0.5,
            sleeper=sleeps.append, rng=rng,
        )
    assert len(sleeps) == 1
    # base * 2^0 = 1.0, jitter ±50% → [0.5, 1.5]
    assert 0.5 <= sleeps[0] <= 1.5
```

▶️ **Run:** `pytest tests/test_retry.py`

✅ **Checkpoint:** `8 passed` — *and instant* (no real sleeping). Whole suite: `pytest` →
**15 passed. Cumulative: 15.** 🏁 **Phase A milestone:** typed, tested foundation complete.

💡 **Why.** The technique that makes retry logic *fast and deterministic to test* is **dependency
injection of the clock and RNG**: `sleeper=sleeps.append` records each delay instead of sleeping, so the
tests *assert the exact backoff schedule* (`[1.0, 2.0]`) in microseconds. `on: tuple[...]` lets the
*caller* decide what's retryable — an unrelated exception propagates immediately with zero sleeps
(`test_retry_does_not_catch_unrelated_exceptions`), so you never retry a bug. `TypeVar("T")` keeps it
generic (it returns whatever `fn` returns). The default behavior re-raises the *original* exception so
callers still catch the specific type; `wrap_final=True` opts into `RetryExhausted`.

> ⚠️ The `_compute_delay` line `min(cap, base * (2 ** attempt))` computes the power *before* the cap, so
> an absurd `attempt` overflows. That's a real, deliberate gap — Phase F's adversarial probe pins it.

---

# PHASE B — The transformation core

The most valuable code with the *least* infrastructure: turn nested JSON into flat, verified CSV. Pure
and offline — at the end of B you can demo real business value with **zero** Kafka or ES.

## ✅ B1 — The transformer (dotted-path projection)

🎯 **Goal:** flatten each ES document into a row of named columns using a tiny path language
(`a.b[0].c`); missing paths become empty strings, but a *mis-configured* path fails loud.

📦 **Depends on:** A2 (`TransformError`).

📄 **Build — `src/etl/transformer.py`:**

```python
"""Per-row JSON-to-flat-row projection.

The original design called for a JOLT transformation pass; we replaced JOLT
with a simpler **dotted-path projection** because no maintained Python port
of JOLT exists. Each CSV column is associated with a dotted path into the
source document (e.g. column ``user_id`` maps to ``user.id``). Missing
paths yield an empty string; columns absent from the mapping fall back to
a same-name top-level lookup.

Supported path syntax:

* ``a``           — top-level key
* ``a.b.c``       — nested object keys
* ``a.b[0]``      — list index (``[N]`` only, no slices)
* ``a.b[0].c``    — mixed

That covers every NiFi-style export we have today. If a job ever needs
something richer (wildcards, conditionals, transformations), add a custom
pre-step in this module rather than re-introducing a JOLT dependency.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from typing import Any

from etl.errors import TransformError

# Splits "users[0].name" → ["users", "[0]", "name"]
_TOKEN_RE = re.compile(r"\[\d+\]|[^.\[]+")
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


def project(
    hit: dict[str, Any],
    columns: list[str],
    column_paths: dict[str, str] | None,
    *,
    job_id: str,
    hit_id: str | None = None,
) -> dict[str, Any]:
    """Project one source dict into a flat {column: value} dict.

    `column_paths` overrides the default name lookup for individual columns.
    Raises `TransformError` if a configured path is syntactically empty.
    """
    paths = column_paths or {}
    out: dict[str, Any] = {}
    for col in columns:
        path = paths.get(col, col)
        if not path:
            raise TransformError(
                f"empty path for column {col!r}", job_id=job_id, hit_id=hit_id,
            )
        out[col] = get_by_path(hit, path)
    return out


def iter_transformed(
    hits: Iterable[dict[str, Any]],
    column_paths: dict[str, str] | None,
    columns: list[str],
    *,
    job_id: str,
) -> Iterator[dict[str, Any]]:
    for hit in hits:
        hit_id = hit.get("_id") if isinstance(hit, dict) else None
        yield project(hit, columns, column_paths, job_id=job_id, hit_id=hit_id)
```

🧪 **Test — `tests/test_transformer.py`:**

```python
from __future__ import annotations

import pytest

from etl.errors import TransformError
from etl.transformer import get_by_path, iter_transformed, project


def test_get_by_path_top_level() -> None:
    assert get_by_path({"a": 1}, "a") == 1


def test_get_by_path_nested() -> None:
    assert get_by_path({"a": {"b": {"c": 7}}}, "a.b.c") == 7


def test_get_by_path_list_index() -> None:
    doc = {"users": [{"id": "u1"}, {"id": "u2"}]}
    assert get_by_path(doc, "users[0].id") == "u1"
    assert get_by_path(doc, "users[1].id") == "u2"


def test_get_by_path_missing_returns_empty_string() -> None:
    assert get_by_path({"a": {"b": 1}}, "a.c") == ""
    assert get_by_path({"a": {"b": 1}}, "missing") == ""
    assert get_by_path({"a": [1]}, "a[5]") == ""


def test_get_by_path_none_returns_empty_string() -> None:
    assert get_by_path({"a": None}, "a") == ""
    assert get_by_path({"a": None}, "a.b") == ""


def test_project_uses_paths_then_falls_back_to_column_name() -> None:
    hit = {"user": {"id": "u1", "name": "Alice"}, "value": 42}
    out = project(
        hit,
        columns=["id", "name", "value", "missing"],
        column_paths={"id": "user.id", "name": "user.name"},
        job_id="j",
    )
    assert out == {"id": "u1", "name": "Alice", "value": 42, "missing": ""}


def test_project_empty_path_raises_transform_error() -> None:
    with pytest.raises(TransformError):
        project({"a": 1}, columns=["a"], column_paths={"a": ""}, job_id="j", hit_id="h")


def test_iter_transformed_streams_rows() -> None:
    hits = [
        {"user": {"id": "u1", "name": "Alice"}, "amount": 1},
        {"user": {"id": "u2", "name": "Bob"}},
    ]
    out = list(iter_transformed(
        hits,
        column_paths={"id": "user.id", "name": "user.name", "amount": "amount"},
        columns=["id", "name", "amount"],
        job_id="j",
    ))
    assert out == [
        {"id": "u1", "name": "Alice", "amount": 1},
        {"id": "u2", "name": "Bob", "amount": ""},
    ]


def test_iter_transformed_with_no_mapping_is_pure_top_level_projection() -> None:
    hits = [{"id": "u1", "name": "Alice", "extra": "ignored"}]
    out = list(iter_transformed(hits, {}, ["id", "name"], job_id="j"))
    assert out == [{"id": "u1", "name": "Alice"}]
```

▶️ **Run:** `pytest tests/test_transformer.py`

✅ **Checkpoint:** `9 passed`. Whole suite: `pytest` → **24 passed. Cumulative: 24.**

💡 **Why.** `iter_transformed` is a **generator** (`yield`): it produces one row at a time as the
consumer pulls, so a million hits never sit in memory at once. The `isinstance` type-guards before each
key/index turn "ragged data" into an empty cell instead of an exception — defensive parsing of untrusted
shapes. The judgment call the tests pin: **tolerate missing *data*** (empty string) but **fail loud on
operator error** (an empty configured path → `TransformError`). The streaming test proves a missing
nested path becomes `""` mid-stream; the no-mapping test proves columns not in the list are dropped.

---

## ✅ B2 — CSV writer + integrity sidecar

🎯 **Goal:** stream rows to a CSV file and compute a SHA256 checksum *in the same pass*, writing a
`sha256sum`-format sidecar so a partner can verify delivery.

📦 **Depends on:** A2 (`CsvWriteError`, `CsvResult`).

📄 **Build — `src/etl/csv_writer.py`:**

```python
"""Streaming CSV writer that computes a SHA256 sidecar in the same pass.

The hash is updated incrementally as bytes are written to disk, so the file
is hashed exactly once regardless of size.
"""

from __future__ import annotations

import csv
import hashlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any, BinaryIO

from etl.errors import CsvWriteError
from etl.models import CsvResult


class _HashingWriter:
    """File wrapper that mirrors writes into a hashlib hasher."""

    def __init__(self, fh: BinaryIO, hasher: Any) -> None:
        self._fh = fh
        self._hasher = hasher

    def write(self, s: str) -> int:
        b = s.encode("utf-8")
        self._fh.write(b)
        self._hasher.update(b)
        return len(s)


def write_csv(
    rows: Iterable[dict[str, Any]],
    columns: list[str],
    csv_path: Path,
) -> CsvResult:
    if not columns:
        raise CsvWriteError("columns must not be empty")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    hasher = hashlib.sha256()
    row_count = 0
    try:
        with csv_path.open("wb") as fh:
            wrapper = _HashingWriter(fh, hasher)
            writer = csv.DictWriter(
                wrapper,
                fieldnames=columns,
                extrasaction="ignore",
                lineterminator="\n",
            )
            writer.writeheader()
            for row in rows:
                writer.writerow({c: _stringify(row.get(c, "")) for c in columns})
                row_count += 1
    except OSError as e:
        raise CsvWriteError(f"failed writing csv {csv_path}: {e!r}") from e

    sidecar_path = csv_path.with_suffix(csv_path.suffix + ".sha256")
    digest = hasher.hexdigest()
    try:
        # sha256sum format: "<hex>  <filename>\n" (two spaces, basename only).
        sidecar_path.write_text(f"{digest}  {csv_path.name}\n", encoding="utf-8")
    except OSError as e:
        raise CsvWriteError(f"failed writing sidecar {sidecar_path}: {e!r}") from e

    return CsvResult(
        csv_path=csv_path,
        sidecar_path=sidecar_path,
        row_count=row_count,
        sha256_hex=digest,
    )


def _stringify(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)
```

🧪 **Test — `tests/test_csv_writer.py`:**

```python
from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

from etl.csv_writer import write_csv
from etl.errors import CsvWriteError


def test_write_csv_basic(tmp_path: Path) -> None:
    rows = [
        {"id": "1", "name": "alpha", "value": 10},
        {"id": "2", "name": "beta",  "value": 20},
        {"id": "3", "name": "gamma", "value": 30},
    ]
    out = tmp_path / "sub" / "out.csv"
    res = write_csv(rows, ["id", "name", "value"], out)

    assert res.row_count == 3
    assert res.csv_path == out
    assert res.sidecar_path == out.with_suffix(".csv.sha256")
    assert out.exists()
    assert res.sidecar_path.exists()

    # Round-trip
    with out.open("r", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        rows_read = list(reader)
    assert rows_read[0] == ["id", "name", "value"]
    assert rows_read[1] == ["1", "alpha", "10"]
    assert rows_read[3] == ["3", "gamma", "30"]

    # Sidecar matches sha256 of the file on disk.
    actual = hashlib.sha256(out.read_bytes()).hexdigest()
    assert res.sha256_hex == actual
    sidecar_text = res.sidecar_path.read_text(encoding="utf-8")
    assert sidecar_text == f"{actual}  {out.name}\n"


def test_write_csv_handles_missing_fields_and_none(tmp_path: Path) -> None:
    rows = [{"id": "1"}, {"id": "2", "name": None}]
    out = tmp_path / "out.csv"
    res = write_csv(rows, ["id", "name"], out)
    assert res.row_count == 2
    body = out.read_text(encoding="utf-8").splitlines()
    assert body == ["id,name", "1,", "2,"]


def test_write_csv_handles_special_characters(tmp_path: Path) -> None:
    rows = [{"id": "1", "name": 'a,"b"\nc'}]
    out = tmp_path / "out.csv"
    write_csv(rows, ["id", "name"], out)
    # The csv module quotes/escapes the special characters; we round-trip to
    # verify the original value is recovered.
    with out.open(newline="", encoding="utf-8") as fh:
        rows_read = list(csv.reader(fh))
    assert rows_read[1] == ["1", 'a,"b"\nc']


def test_write_csv_empty_columns_raises(tmp_path: Path) -> None:
    with pytest.raises(CsvWriteError, match="columns"):
        write_csv([], [], tmp_path / "out.csv")


def test_write_csv_zero_rows_writes_header_only(tmp_path: Path) -> None:
    out = tmp_path / "out.csv"
    res = write_csv(iter([]), ["a", "b"], out)
    assert res.row_count == 0
    assert out.read_text(encoding="utf-8") == "a,b\n"
```

▶️ **Run:** `pytest tests/test_csv_writer.py`

✅ **Checkpoint:** `5 passed`. Whole suite: `pytest` → **29 passed. Cumulative: 29.**
🏁 **Phase B milestone (Stage 0 — shippable offline value):** you can flatten nested documents into a
verified CSV with *zero infrastructure*. Wire `iter_transformed → write_csv` over a list of sample dicts
in a throwaway script and you have your first end-to-end demo.

💡 **Why.** `_HashingWriter` is the **decorator pattern at the I/O level**: `csv.DictWriter` only needs
something with a `.write(str)` method (duck typing), so we wrap the real file to *also* feed every byte
to the hasher. The file is therefore hashed **once, incrementally, as it's written** — no second read
pass. Binary mode + `lineterminator="\n"` make the bytes (and thus the hash) identical across platforms.
The crucial assertion is in `test_write_csv_basic`: the reported `sha256_hex` equals an **independent**
recompute of the file's bytes — i.e. exactly what `sha256sum -c` would compute downstream. The
zero-rows test passing an `iter([])` proves it truly streams an *iterator*, not just a list.

> ⚠️ Two deliberate gaps Phase F pins: `write` returns `len(s)` (chars) not bytes, and a cell starting
> with `=` is written raw (spreadsheet formula-injection vector).

---

# PHASE C — Elasticsearch

Now the data plane: page through a query efficiently, bridge it into the app, and turn an external job
document into a trusted `JobSpec`.

## ✅ C1 — PIT + `search_after` pagination (the standalone `es_extract` package)

🎯 **Goal:** stream every hit of a query out of Elasticsearch without loading it all into memory or
hitting deep-pagination limits — packaged as a **standalone, reusable extractor** that imports *nothing*
from `etl`.

📦 **Depends on:** A1 (it's a fresh package; only the stdlib + a duck-typed ES client).

🔍 **ES concept — why not `from`/`size`?** Naive offset pagination forces ES to find and discard the
first *N* on every shard per request (cost grows with depth; capped by `index.max_result_window`,
default 10,000) and can skip/duplicate rows if data changes mid-scan. Elastic's recommended fix is a
**point-in-time** (a consistent snapshot) plus **`search_after`** (a stable cursor): `sort` by
`_shard_doc` (cheapest stable tie-breaker, valid only inside a PIT), pass the previous page's last sort
value as `search_after`, and always `close_point_in_time` when done. `track_total_hits: False` skips the
expensive exact-total (the validator gets the authoritative number from `_count`).

📄 **Build the package — five files under `src/es_extract/`.** Build them in this order so each import
resolves.

**`src/es_extract/errors.py`:**

```python
"""Error type for the standalone ES-extraction package.

The functions and strategies here let the *caller* inject which exception type
wraps a failure (`error_cls`), defaulting to `EsExtractError`. That keeps this
package free of any dependency on a host application's error hierarchy: a host
(like this repo's ``etl`` package) can pass its own ``ElasticsearchQueryError``
so failures land in its existing ``except`` boundary, while a standalone user
gets a plain ``EsExtractError``.
"""

from __future__ import annotations


class EsExtractError(Exception):
    """Raised on an Elasticsearch request failure during extraction."""
```

**`src/es_extract/diagnostics.py`:**

```python
"""Capture the raw hit stream to disk for diagnostics — without buffering it.

``tee_to_ndjson`` wraps any hit iterator: it yields each hit through unchanged
while appending it as one JSON object per line (NDJSON). Because it is a
generator that holds the file open for its own lifetime, it stays
memory-bounded (one hit at a time) and the file is flushed/closed when
iteration finishes *or* the consumer abandons it. Drop it into a pipeline to
record exactly what flowed through.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any


def tee_to_ndjson(
    hits: Iterable[dict[str, Any]], path: Path
) -> Iterator[dict[str, Any]]:
    """Yield each hit unchanged while writing it as NDJSON to ``path``."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for hit in hits:
            fh.write(json.dumps(hit, ensure_ascii=False, default=str))
            fh.write("\n")
            yield hit


def dump_to_ndjson(hits: Iterable[dict[str, Any]], path: Path) -> int:
    """Eagerly write every hit to ``path`` as NDJSON; return the count written."""
    written = 0
    for _ in tee_to_ndjson(hits, path):
        written += 1
    return written
```

**`src/es_extract/pagination.py`** (the heart — one resource-owning generator):

```python
"""Point-in-time + ``search_after`` Elasticsearch pagination.

A single streaming strategy: :class:`SearchAfterPagination` opens a
point-in-time (PIT), pages through every matching hit with ``search_after``,
and **always** closes the PIT in a ``finally`` block — even if the consumer
abandons iteration early. It is a generator, so it stays memory-bounded (one
page at a time) regardless of how large the result set is.

It is duck-typed against the official ``elasticsearch`` client but accepts any
object exposing ``open_point_in_time`` / ``search`` / ``close_point_in_time``,
so it is trivially testable with a fake.

Two knobs distinguish it from an application-specific extractor:

* ``source_only`` — when ``True`` (default) yield each hit's ``_source``; when
  ``False`` yield the full hit envelope (``_id``, ``_score``, ``sort``, …).
* ``error_cls`` — the exception type a request failure is wrapped in, so a host
  application can map failures into its own hierarchy (see ``errors.py``).
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from es_extract.errors import EsExtractError

_log = logging.getLogger(__name__)


def _emit(hit: dict[str, Any], *, source_only: bool) -> dict[str, Any]:
    """Return the hit's ``_source`` (default) or the whole envelope."""
    if source_only:
        source: Any = hit.get("_source", {})
        return source if isinstance(source, dict) else {}
    return hit


@dataclass
class SearchAfterPagination:
    """Point-in-time + ``search_after`` pagination (Elastic's recommended
    deep-pagination mechanism).

    Each call to :meth:`iter_hits` owns one PIT for the lifetime of the
    generator and releases it on completion *or* early abandonment.
    """

    keep_alive: str = "5m"
    source_only: bool = True
    error_cls: type[Exception] = EsExtractError

    def iter_hits(
        self, *, es: Any, index: str, query: dict[str, Any], page_size: int
    ) -> Iterator[dict[str, Any]]:
        try:
            pit = es.open_point_in_time(index=index, keep_alive=self.keep_alive)
        except Exception as e:
            raise self.error_cls(f"open_point_in_time failed: {e!r}") from e
        pit_id: str | None = pit.get("id")
        try:
            search_after: list[Any] | None = None
            while True:
                body: dict[str, Any] = {
                    "size": page_size,
                    "query": query,
                    "pit": {"id": pit_id, "keep_alive": self.keep_alive},
                    "sort": [{"_shard_doc": "asc"}],
                    "track_total_hits": False,
                }
                if search_after is not None:
                    body["search_after"] = search_after
                try:
                    resp = es.search(body=body)
                except Exception as e:
                    raise self.error_cls(f"search_after page failed: {e!r}") from e
                pit_id = resp.get("pit_id", pit_id)
                hits = resp.get("hits", {}).get("hits", [])
                if not hits:
                    break
                for h in hits:
                    yield _emit(h, source_only=self.source_only)
                last_sort = hits[-1].get("sort")
                if not last_sort:
                    break
                search_after = last_sort
        finally:
            if pit_id is not None:
                try:
                    es.close_point_in_time(body={"id": pit_id})
                except Exception as e:  # best-effort: the PIT keep-alive reaps it anyway
                    _log.warning("close_point_in_time failed: %r", e)
```

**`src/es_extract/extract.py`** (the `_count` ground truth + a one-call helper):

```python
"""Top-level extraction helpers: ``count`` and a one-call ``iter_hits``.

``iter_hits`` is the convenience entry point for callers who just want "stream
every hit this query matches" without constructing a strategy by hand. It
paginates with point-in-time + ``search_after`` (see :mod:`es_extract.pagination`).
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from es_extract.errors import EsExtractError
from es_extract.pagination import SearchAfterPagination


def count(
    es: Any, index: str, query: dict[str, Any], *,
    error_cls: type[Exception] = EsExtractError,
) -> int:
    """Return how many documents ``query`` matches in ``index`` (``_count``)."""
    try:
        resp = es.count(index=index, body={"query": query})
    except Exception as e:
        raise error_cls(f"count failed: {e!r}") from e
    return int(resp.get("count", 0))


def iter_hits(
    es: Any, index: str, query: dict[str, Any], *,
    page_size: int = 1000, keep_alive: str = "5m", source_only: bool = True,
    error_cls: type[Exception] = EsExtractError,
) -> Iterator[dict[str, Any]]:
    """Stream every hit ``query`` matches via point-in-time + ``search_after``.

    ``keep_alive`` / ``source_only`` / ``error_cls`` configure the underlying
    :class:`~es_extract.pagination.SearchAfterPagination`. Pass
    ``source_only=False`` to receive the full hit envelope (``_id``, ``_score``,
    ``sort``) instead of just ``_source``.
    """
    strategy = SearchAfterPagination(
        keep_alive=keep_alive, source_only=source_only, error_cls=error_cls
    )
    return strategy.iter_hits(es=es, index=index, query=query, page_size=page_size)
```

**`src/es_extract/__init__.py`** (the public surface — replaces the empty marker from A1):

```python
"""Standalone, dependency-light Elasticsearch extraction.

Depends only on the standard library and a duck-typed Elasticsearch client
(any object exposing ``count`` / ``search`` / ``open_point_in_time`` /
``close_point_in_time``). It has **no** dependency on the rest of this
repository, so the package can be copied or installed and reused on its own.

Extraction uses point-in-time + ``search_after`` — Elastic's recommended
deep-pagination mechanism — exposed as a memory-bounded streaming generator.

Quick start::

    from elasticsearch import Elasticsearch
    from es_extract import count, iter_hits

    es = Elasticsearch("http://localhost:9200")
    q = {"match_all": {}}
    print(count(es, "my-index", q))
    for src in iter_hits(es, "my-index", q):
        ...  # `src` is each hit's _source dict (pass source_only=False for the envelope)
"""

from __future__ import annotations

from es_extract.diagnostics import dump_to_ndjson, tee_to_ndjson
from es_extract.errors import EsExtractError
from es_extract.extract import count, iter_hits
from es_extract.pagination import SearchAfterPagination

__all__ = [
    "EsExtractError",
    "SearchAfterPagination",
    "count",
    "dump_to_ndjson",
    "iter_hits",
    "tee_to_ndjson",
]

__version__ = "0.2.0"
```

🧪 **Test — `tests/test_es_extract.py`** (note: imports **only** from `es_extract`, never `etl` — the
decoupling is enforced by the test itself):

```python
"""Tests for the standalone `es_extract` package (no `etl` imports)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from es_extract import (
    EsExtractError,
    SearchAfterPagination,
    count,
    dump_to_ndjson,
    iter_hits,
    tee_to_ndjson,
)


def _hit(src: dict[str, Any], sort: list[Any] | None = None) -> dict[str, Any]:
    h: dict[str, Any] = {"_source": src, "_id": src.get("id")}
    if sort is not None:
        h["sort"] = sort
    return h


# --- count ---------------------------------------------------------------

def test_count_returns_int() -> None:
    es = MagicMock()
    es.count.return_value = {"count": 7}
    assert count(es, "i", {"match_all": {}}) == 7
    es.count.assert_called_once_with(index="i", body={"query": {"match_all": {}}})


def test_count_wraps_errors_with_default() -> None:
    es = MagicMock()
    es.count.side_effect = RuntimeError("boom")
    with pytest.raises(EsExtractError):
        count(es, "i", {})


def test_count_wraps_errors_with_injected_error_cls() -> None:
    class MyErr(Exception):
        pass

    es = MagicMock()
    es.count.side_effect = RuntimeError("boom")
    with pytest.raises(MyErr):
        count(es, "i", {}, error_cls=MyErr)


# --- search_after pagination ---------------------------------------------

def test_search_after_pages_and_closes_pit() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = [
        {"pit_id": "pit-1",
         "hits": {"hits": [_hit({"id": 1}, [1]), _hit({"id": 2}, [2])]}},
        {"pit_id": "pit-1", "hits": {"hits": [_hit({"id": 3}, [3])]}},
        {"pit_id": "pit-1", "hits": {"hits": []}},
    ]
    out = list(SearchAfterPagination().iter_hits(es=es, index="i", query={}, page_size=2))
    assert out == [{"id": 1}, {"id": 2}, {"id": 3}]
    # search_after threads the previous page's last sort value through.
    bodies = [c.kwargs["body"] for c in es.search.call_args_list]
    assert "search_after" not in bodies[0]
    assert bodies[1]["search_after"] == [2]
    assert bodies[2]["search_after"] == [3]
    es.open_point_in_time.assert_called_once_with(index="i", keep_alive="5m")
    es.close_point_in_time.assert_called_once_with(body={"id": "pit-1"})


def test_search_after_source_only_false_yields_full_envelope() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = [
        {"pit_id": "pit-1", "hits": {"hits": [_hit({"id": 1}, [1])]}},
        {"pit_id": "pit-1", "hits": {"hits": []}},
    ]
    out = list(
        SearchAfterPagination(source_only=False).iter_hits(
            es=es, index="i", query={}, page_size=1
        )
    )
    assert out == [{"_source": {"id": 1}, "_id": 1, "sort": [1]}]  # envelope incl. _id


def test_search_after_closes_pit_on_early_close() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.return_value = {
        "pit_id": "pit-1",
        "hits": {"hits": [_hit({"id": 1}, [1]), _hit({"id": 2}, [2])]},
    }
    gen = SearchAfterPagination().iter_hits(es=es, index="i", query={}, page_size=2)
    next(gen)
    gen.close()
    es.close_point_in_time.assert_called_once_with(body={"id": "pit-1"})


def test_search_after_open_error_does_not_close() -> None:
    es = MagicMock()
    es.open_point_in_time.side_effect = RuntimeError("denied")
    with pytest.raises(EsExtractError):
        list(SearchAfterPagination().iter_hits(es=es, index="i", query={}, page_size=1))
    es.close_point_in_time.assert_not_called()


def test_search_after_wraps_errors_with_injected_cls() -> None:
    class MyErr(Exception):
        pass

    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = RuntimeError("down")
    with pytest.raises(MyErr):
        list(
            SearchAfterPagination(error_cls=MyErr).iter_hits(
                es=es, index="i", query={}, page_size=2
            )
        )
    es.close_point_in_time.assert_called_once_with(body={"id": "pit-1"})


# --- one-call iter_hits convenience --------------------------------------

def test_iter_hits_convenience_streams_via_pit() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = [
        {"pit_id": "pit-1", "hits": {"hits": [_hit({"id": 1}, [1])]}},
        {"pit_id": "pit-1", "hits": {"hits": []}},
    ]
    out = list(iter_hits(es, "i", {}, page_size=5))
    assert out == [{"id": 1}]
    es.open_point_in_time.assert_called_once_with(index="i", keep_alive="5m")


# --- diagnostics ---------------------------------------------------------

def test_tee_to_ndjson_passes_through_and_writes(tmp_path: Path) -> None:
    path = tmp_path / "sub" / "dump.ndjson"
    src = [{"a": 1}, {"a": 2}]
    out = list(tee_to_ndjson(iter(src), path))
    assert out == src  # yielded unchanged
    lines = path.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == src


def test_dump_to_ndjson_returns_count(tmp_path: Path) -> None:
    path = tmp_path / "dump.ndjson"
    assert dump_to_ndjson(iter([{"a": 1}, {"a": 2}, {"a": 3}]), path) == 3
    assert len(path.read_text(encoding="utf-8").splitlines()) == 3
```

▶️ **Run:** `pytest tests/test_es_extract.py`

✅ **Checkpoint:** `11 passed`. Whole suite: `pytest` → **40 passed. Cumulative: 40.**

💡 **Why.** The make-or-break property is **resource-owning generators clean up in `finally`**. A PIT is
a *server-side* resource; the `try/finally` runs `close_point_in_time` even if the consumer abandons
iteration early — proven by `test_search_after_closes_pit_on_early_close` (pull one hit, `gen.close()`,
assert the PIT closed). The test technique throughout is `es.search.side_effect = [page, page, empty]`:
consecutive calls return scripted pages, and the test asserts the **cursor threading** (no `search_after`
on page 1; each later body carries the previous page's last `sort`). `error_cls` injection is verified
both ways — default `EsExtractError` and a custom class — which is exactly the seam `etl` uses next.

> 💡 Because this package has no `etl` import, you can also exercise it against a *real* cluster on its
> own with `scripts/try_es_extract.py --seed --cleanup` (Phase G), which cross-checks the streamed count
> against `_count`.

---

## ✅ C2 — The ES bridge (`etl/extractor.py`)

🎯 **Goal:** a thin adapter that owns the app's `_count` and hit stream, pinning every ES failure to
`ElasticsearchQueryError` so they land in the daemon's single `EtlError` boundary.

📦 **Depends on:** C1 (`es_extract`), A2 (`ElasticsearchQueryError`, `JobSpec`).

📄 **Build — `src/etl/extractor.py`:**

```python
"""Bridges the ES client to the standalone extraction package.

The extractor is intentionally thin: it owns the ``_count`` call and the hit
stream, delegating both to :mod:`es_extract` with failures pinned to
:class:`etl.errors.ElasticsearchQueryError` so they land in the daemon's single
``EtlError`` boundary. Extraction paginates with point-in-time + ``search_after``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from es_extract import count as _count
from es_extract import iter_hits as _iter_hits
from etl.errors import ElasticsearchQueryError
from etl.models import JobSpec


def expected_count(es: Any, index: str, query: dict[str, Any]) -> int:
    return _count(es, index, query, error_cls=ElasticsearchQueryError)


def iter_hits(
    es: Any,
    job: JobSpec,
    *,
    page_size: int,
    keep_alive: str = "5m",
) -> Iterator[dict[str, Any]]:
    """Stream a job's hits as ``_source`` dicts via PIT + ``search_after``."""
    return _iter_hits(
        es,
        job.data_index,
        job.query,
        page_size=page_size,
        keep_alive=keep_alive,
        error_cls=ElasticsearchQueryError,
    )
```

🧪 **Test — `tests/test_extractor.py`:**

```python
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from etl.errors import ElasticsearchQueryError
from etl.extractor import expected_count, iter_hits
from etl.models import JobSpec


def _job() -> JobSpec:
    return JobSpec(
        job_id="j", data_index="d", query={"q": 1}, column_paths={},
        columns=["a"], remote_filename="r.csv",
    )


def test_expected_count_returns_int() -> None:
    es = MagicMock()
    es.count.return_value = {"count": 42}
    assert expected_count(es, "i", {"match_all": {}}) == 42
    es.count.assert_called_once_with(index="i", body={"query": {"match_all": {}}})


def test_expected_count_wraps_errors() -> None:
    es = MagicMock()
    es.count.side_effect = RuntimeError("boom")
    with pytest.raises(ElasticsearchQueryError):
        expected_count(es, "i", {})


def test_iter_hits_streams_source_via_pit() -> None:
    es = MagicMock()
    es.open_point_in_time.return_value = {"id": "pit-1"}
    es.search.side_effect = [
        {"pit_id": "pit-1", "hits": {"hits": [{"_source": {"a": 1}, "sort": [1]},
                                              {"_source": {"a": 2}, "sort": [2]}]}},
        {"pit_id": "pit-1", "hits": {"hits": []}},
    ]
    out = list(iter_hits(es, _job(), page_size=10, keep_alive="2m"))
    assert out == [{"a": 1}, {"a": 2}]
    # job.data_index + keep_alive thread through to the PIT open …
    es.open_point_in_time.assert_called_once_with(index="d", keep_alive="2m")
    # … and job.query lands in the search body.
    assert es.search.call_args_list[0].kwargs["body"]["query"] == {"q": 1}


def test_iter_hits_wraps_errors_in_elasticsearch_query_error() -> None:
    es = MagicMock()
    es.open_point_in_time.side_effect = RuntimeError("down")
    with pytest.raises(ElasticsearchQueryError):
        list(iter_hits(es, _job(), page_size=10))
```

▶️ **Run:** `pytest tests/test_extractor.py`

✅ **Checkpoint:** `4 passed`. Whole suite: `pytest` → **44 passed. Cumulative: 44.**

💡 **Why.** This is **dependency injection applied to the error taxonomy**: `es_extract` stays free of
any `etl` import, while `etl` passes `ElasticsearchQueryError` so failures fold into its one boundary.
`iter_hits` unpacks a `JobSpec` (callers pass a *job*, not loose fields). The test proves the job's
`data_index`/`keep_alive` thread into `open_point_in_time` and its `query` into the search body — i.e.
the bridge is wiring the right values, not just returning the right result.

---

## ✅ C3 — Job loader (validate at the boundary)

🎯 **Goal:** fetch a job document by id and validate *every* field — types included — into a trustworthy
`JobSpec`. After this returns, downstream code never re-checks.

📦 **Depends on:** A2 (`ElasticsearchQueryError`, `JobSpecError`, `JobSpec`).

📄 **Build — `src/etl/job_loader.py`:**

```python
"""Resolves a control message's `job_doc_id` to a `JobSpec` from Elasticsearch."""

from __future__ import annotations

from typing import Any

from etl.errors import ElasticsearchQueryError, JobSpecError
from etl.models import JobSpec

_REQUIRED = ("data_index", "query", "columns", "remote_filename")


def load_job(es: Any, *, job_index: str, job_doc_id: str) -> JobSpec:
    try:
        doc = es.get(index=job_index, id=job_doc_id)
    except Exception as e:
        raise ElasticsearchQueryError(
            f"failed to GET job doc {job_doc_id} from {job_index}: {e!r}"
        ) from e

    if not doc.get("found", True):
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

    column_paths = source.get("column_paths", {})
    if column_paths is None:
        column_paths = {}
    if not isinstance(column_paths, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in column_paths.items()
    ):
        raise JobSpecError(f"job doc {job_doc_id}: 'column_paths' must be dict[str, str]")

    remote_filename = source["remote_filename"]
    if not isinstance(remote_filename, str) or not remote_filename:
        raise JobSpecError(f"job doc {job_doc_id}: 'remote_filename' must be non-empty string")

    return JobSpec(
        job_id=str(source.get("job_id", job_doc_id)),
        data_index=str(source["data_index"]),
        query=query,
        column_paths=column_paths,
        columns=list(columns),
        remote_filename=remote_filename,
    )
```

🧪 **Test — `tests/test_job_loader.py`:**

```python
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from etl.errors import ElasticsearchQueryError, JobSpecError
from etl.job_loader import load_job


def _doc(source: dict) -> dict:
    return {"_id": "x", "found": True, "_source": source}


def test_load_job_happy_path() -> None:
    es = MagicMock()
    es.get.return_value = _doc({
        "job_id": "job-7",
        "data_index": "sales-2024",
        "query": {"match_all": {}},
        "column_paths": {"x": "user.id", "y": "amount"},
        "columns": ["x", "y"],
        "remote_filename": "exports/job-7.csv",
    })
    spec = load_job(es, job_index="jobs", job_doc_id="job-7")
    assert spec.job_id == "job-7"
    assert spec.data_index == "sales-2024"
    assert spec.columns == ["x", "y"]
    assert spec.column_paths == {"x": "user.id", "y": "amount"}


def test_load_job_missing_doc_raises() -> None:
    es = MagicMock()
    es.get.return_value = {"found": False}
    with pytest.raises(JobSpecError, match="not found"):
        load_job(es, job_index="jobs", job_doc_id="absent")


def test_load_job_missing_fields_raises() -> None:
    es = MagicMock()
    es.get.return_value = _doc({"data_index": "i"})
    with pytest.raises(JobSpecError, match="missing"):
        load_job(es, job_index="jobs", job_doc_id="bad")


def test_load_job_bad_columns_raises() -> None:
    es = MagicMock()
    es.get.return_value = _doc({
        "data_index": "i",
        "query": {},
        "columns": ["a", 1, "b"],
        "remote_filename": "r.csv",
    })
    with pytest.raises(JobSpecError, match="columns"):
        load_job(es, job_index="jobs", job_doc_id="bad")


def test_load_job_es_error_wrapped() -> None:
    es = MagicMock()
    es.get.side_effect = RuntimeError("network")
    with pytest.raises(ElasticsearchQueryError):
        load_job(es, job_index="jobs", job_doc_id="x")


def test_load_job_defaults_empty_column_paths() -> None:
    es = MagicMock()
    es.get.return_value = _doc({
        "data_index": "i",
        "query": {},
        "columns": ["a"],
        "remote_filename": "r.csv",
    })
    spec = load_job(es, job_index="jobs", job_doc_id="x")
    assert spec.column_paths == {}


def test_load_job_bad_column_paths_raises() -> None:
    es = MagicMock()
    es.get.return_value = _doc({
        "data_index": "i",
        "query": {},
        "columns": ["a"],
        "remote_filename": "r.csv",
        "column_paths": {"a": 5},  # value must be str
    })
    with pytest.raises(JobSpecError, match="column_paths"):
        load_job(es, job_index="jobs", job_doc_id="x")
```

▶️ **Run:** `pytest tests/test_job_loader.py`

✅ **Checkpoint:** `7 passed`. Whole suite: `pytest` → **51 passed. Cumulative: 51.**
🏁 **Phase C milestone:** with a real ES stack (Phase G) you can extract live data end-to-end.

💡 **Why.** **Validate-at-the-boundary:** a job document is external, untyped data an operator wrote, so
the moment it enters the typed core we check every field and convert failures into a precise
`JobSpecError`. Note the two *distinct* failure classes the tests separate: a raising `es.get` →
`ElasticsearchQueryError` (network), but a `{"found": false}` or malformed shape → `JobSpecError`
(data). `all(isinstance(...) for ...)` is a generator-expression "every element is a string" check.

> ⚠️ Deliberate gap (Phase F): an *empty* `columns: []` passes here (`all(...)` is vacuously true) and
> only fails later in `write_csv`.

---

# PHASE D — Trust & delivery

## ✅ D1 — The validator (two-tier count check)

🎯 **Goal:** guarantee the CSV row count equals what ES reports — *tolerating* transient refresh races,
but *failing loud* on real loss.

📦 **Depends on:** A3 (`RetryConfig`), A2 (`RecordCountMismatch`, `CsvResult`), C2 (`expected_count`).

🔍 **ES concept — near-real-time & refresh races.** ES is *near*-real-time: newly indexed docs become
searchable only after a *refresh* (~1s). `_count` and a paginated read can observe the index a moment
apart, so a *transient* mismatch may be a race, not loss. We must neither cry wolf on the first mismatch
nor ship a wrong file.

📄 **Build — `src/etl/validator.py`:**

```python
"""Two-tier record-count validation.

Tier 1: re-query ES `_count` up to N times with exp backoff. Handles cases
        where a refresh races the initial count read.
Tier 2: one full extract+CSV re-run via a caller-supplied callback.

If both tiers still disagree with the on-disk CSV row count, raise
`RecordCountMismatch` with the full attempt history.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Callable
from typing import Any

from etl.config import RetryConfig
from etl.errors import RecordCountMismatch
from etl.extractor import expected_count
from etl.models import CsvResult

_log = logging.getLogger(__name__)


def validate_counts(expected: int, actual: int) -> None:
    if expected != actual:
        raise RecordCountMismatch(expected=expected, actual=actual,
                                  attempts=[(expected, actual)])


def validate_with_retry(
    *,
    es: Any,
    index: str,
    query: dict[str, Any],
    csv_result: CsvResult,
    retry_cfg: RetryConfig,
    on_full_reextract: Callable[[], CsvResult],
    sleeper: Callable[[float], None] | None = None,
    rng: random.Random | None = None,
    log_extra: dict[str, Any] | None = None,
) -> CsvResult:
    """Validate counts with the two-tier retry strategy.

    Returns the `CsvResult` corresponding to the file that ultimately matched
    (may be the re-extracted one).
    """
    import time as _time
    sleeper = sleeper or _time.sleep
    rng = rng or random.Random()
    attempts_log: list[tuple[int, int]] = []
    current = csv_result

    # Tier 1: re-query _count up to N times.
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
        _log.warning(
            "count mismatch attempt=%d/%d es_count=%d csv_rows=%d delay=%.3fs",
            attempt + 1, retry_cfg.max_attempts, es_count, current.row_count, delay,
            extra={**(log_extra or {}), "retry_attempt": attempt + 1,
                   "retry_delay_s": delay, "es_count": es_count,
                   "csv_rows": current.row_count},
        )
        sleeper(max(0.0, delay))

    # Tier 2: one full extract+CSV re-run.
    _log.warning(
        "count still mismatched after %d retries; running full re-extract",
        retry_cfg.max_attempts, extra=log_extra,
    )
    current = on_full_reextract()
    final_es_count = expected_count(es, index, query)
    attempts_log.append((final_es_count, current.row_count))
    if final_es_count != current.row_count:
        raise RecordCountMismatch(
            expected=final_es_count,
            actual=current.row_count,
            attempts=attempts_log,
        )
    return current
```

🧪 **Test — `tests/test_validator.py`** (uses the `retry_cfg_fast` fixture from conftest):

```python
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from etl.config import RetryConfig
from etl.errors import RecordCountMismatch
from etl.models import CsvResult
from etl.validator import validate_counts, validate_with_retry


def _csv_result(rows: int) -> CsvResult:
    return CsvResult(
        csv_path=Path("/tmp/x.csv"),
        sidecar_path=Path("/tmp/x.csv.sha256"),
        row_count=rows,
        sha256_hex="deadbeef",
    )


def test_validate_counts_equal_ok() -> None:
    validate_counts(5, 5)


def test_validate_counts_unequal_raises() -> None:
    with pytest.raises(RecordCountMismatch):
        validate_counts(5, 4)


def test_validate_with_retry_first_count_matches_no_reextract(
    retry_cfg_fast: RetryConfig,
) -> None:
    es = MagicMock()
    es.count.return_value = {"count": 3}
    reextract = MagicMock(side_effect=AssertionError("should not be called"))

    res = validate_with_retry(
        es=es,
        index="i",
        query={"match_all": {}},
        csv_result=_csv_result(3),
        retry_cfg=retry_cfg_fast,
        on_full_reextract=reextract,
        sleeper=lambda _: None,
    )
    assert res.row_count == 3
    assert es.count.call_count == 1
    reextract.assert_not_called()


def test_validate_with_retry_recovers_after_a_couple_flaps(
    retry_cfg_fast: RetryConfig,
) -> None:
    es = MagicMock()
    es.count.side_effect = [
        {"count": 99},  # mismatch
        {"count": 99},  # mismatch
        {"count": 3},   # match
    ]
    reextract = MagicMock(side_effect=AssertionError("should not be called"))
    res = validate_with_retry(
        es=es, index="i", query={},
        csv_result=_csv_result(3),
        retry_cfg=retry_cfg_fast,
        on_full_reextract=reextract,
        sleeper=lambda _: None,
    )
    assert res.row_count == 3
    assert es.count.call_count == 3
    reextract.assert_not_called()


def test_validate_with_retry_triggers_reextract_and_succeeds(
    retry_cfg_fast: RetryConfig,
) -> None:
    es = MagicMock()
    # First 5 _count attempts: all mismatch.
    # 6th call (post-reextract verification): match.
    es.count.side_effect = [{"count": 99}] * 5 + [{"count": 7}]
    reextract = MagicMock(return_value=_csv_result(7))

    res = validate_with_retry(
        es=es, index="i", query={},
        csv_result=_csv_result(3),
        retry_cfg=retry_cfg_fast,
        on_full_reextract=reextract,
        sleeper=lambda _: None,
    )
    assert res.row_count == 7
    reextract.assert_called_once()
    assert es.count.call_count == 6


def test_validate_with_retry_final_mismatch_raises(retry_cfg_fast: RetryConfig) -> None:
    es = MagicMock()
    es.count.side_effect = [{"count": 99}] * 5 + [{"count": 99}]
    reextract = MagicMock(return_value=_csv_result(3))

    with pytest.raises(RecordCountMismatch) as ei:
        validate_with_retry(
            es=es, index="i", query={},
            csv_result=_csv_result(3),
            retry_cfg=retry_cfg_fast,
            on_full_reextract=reextract,
            sleeper=lambda _: None,
        )
    # Tier 1 (5 retries) + tier 2 (1 attempt) → 6 entries in the history.
    assert len(ei.value.attempts) == 6
    assert ei.value.expected == 99
    assert ei.value.actual == 3
```

▶️ **Run:** `pytest tests/test_validator.py`

✅ **Checkpoint:** `6 passed`. Whole suite: `pytest` → **57 passed. Cumulative: 57.**

💡 **Why.** Two design judgments the tests pin. First, **inversion of control via a callback**: the
validator doesn't know *how* to re-extract (that would couple it to the extractor, pagination, and CSV
writer) — it accepts `on_full_reextract: Callable[[], CsvResult]` and calls back. Second, this loop is
**hand-rolled, not `retry_call`**, because it retries on a *value comparison* (`es_count == row_count`),
not an exception. The `side_effect = [...]` lists script a *sequence* of `_count` results across
retries: a Tier-1 recovery (flap-then-match, no re-extract), a Tier-2 success (5 mismatches → re-extract
→ match), and a final failure whose `attempts` history has exactly 6 entries — fully auditable.

---

## ✅ D2 — SFTP uploader

🎯 **Goal:** deliver the CSV + sidecar over SFTP *securely, with retry*, by shelling out to the system
`sftp` binary (no paramiko) and enforcing strict host-key checking.

📦 **Depends on:** A3 (`RetryConfig`, `SftpConfig`), A2 (`SftpUploadError`), A5 (`retry_call`).

📄 **Build — `src/etl/sftp_uploader.py`:**

```python
"""SFTP upload via the system `sftp` binary.

We intentionally shell out (no `paramiko`) and force strict host-key checking
against a user-supplied `known_hosts` file. The batch file is written into a
temp dir, used with `-b`, then removed.
"""

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
    """Pairs each local file with its remote destination path."""

    local: Path
    remote: str


def _build_batch(plans: list[UploadPlan]) -> str:
    lines: list[str] = []
    for p in plans:
        # `sftp` batch files use whitespace as the separator. The shlex.quote
        # call protects against spaces in paths; bare metacharacters in
        # filenames are otherwise harmless here (no shell involved).
        lines.append(f"put {shlex.quote(str(p.local))} {shlex.quote(p.remote)}")
    lines.append("bye")
    return "\n".join(lines) + "\n"


def _run_sftp(cfg: SftpConfig, batch_text: str, *, timeout: float) -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".sftpbatch", delete=True, encoding="utf-8"
    ) as batch_fh:
        batch_fh.write(batch_text)
        batch_fh.flush()
        argv = [
            "sftp",
            "-b", batch_fh.name,
            "-i", str(cfg.key_path),
            "-P", str(cfg.port),
            "-o", f"UserKnownHostsFile={cfg.known_hosts}",
            "-o", "StrictHostKeyChecking=yes",
            "-o", "BatchMode=yes",
            f"{cfg.user}@{cfg.host}",
        ]
        _log.info("sftp invoking", extra={"argv": argv})
        try:
            proc = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as e:
            raise SftpUploadError(f"sftp timed out after {timeout}s: {e!r}") from e
        except FileNotFoundError as e:
            raise SftpUploadError(f"sftp binary not found: {e!r}") from e
        if proc.returncode != 0:
            raise SftpUploadError(
                f"sftp exit={proc.returncode} stderr={proc.stderr.decode('utf-8', 'replace')!r}"
            )


def upload(
    cfg: SftpConfig,
    plans: list[UploadPlan],
    *,
    retry_cfg: RetryConfig,
    timeout: float = 300.0,
    sleeper: Callable[[float], None] | None = None,
) -> None:
    """Upload a set of files via sftp with retry-on-failure."""
    batch_text = _build_batch(plans)
    retry_call(
        _run_sftp,
        cfg,
        batch_text,
        timeout=timeout,
        on=(SftpUploadError,),
        attempts=retry_cfg.max_attempts,
        base=retry_cfg.backoff_base,
        cap=retry_cfg.backoff_cap,
        jitter=retry_cfg.jitter,
        sleeper=sleeper if sleeper is not None else time.sleep,
    )
```

🧪 **Test — `tests/test_sftp_uploader.py`** (monkeypatch the subprocess; never open a socket):

```python
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from etl.config import RetryConfig, SftpConfig
from etl.errors import SftpUploadError
from etl.sftp_uploader import UploadPlan, _build_batch, upload


def _cfg(tmp_path: Path) -> SftpConfig:
    return SftpConfig(
        host="sftp.example.com",
        port=2222,
        user="etl",
        key_path=tmp_path / "id_ed25519",
        remote_dir="/incoming",
        known_hosts=tmp_path / "known_hosts",
    )


def test_build_batch_quotes_paths() -> None:
    plans = [
        UploadPlan(local=Path("/local/a b.csv"), remote="/remote/a b.csv"),
        UploadPlan(local=Path("/local/sha"), remote="/remote/sha"),
    ]
    text = _build_batch(plans)
    assert "'/local/a b.csv'" in text
    assert "'/remote/a b.csv'" in text
    assert text.strip().endswith("bye")


def test_upload_invokes_sftp_with_strict_host_checking(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    captured: dict[str, Any] = {}

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        captured["argv"] = argv
        return MagicMock(returncode=0, stderr=b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    cfg = _cfg(tmp_path)
    upload(
        cfg,
        [UploadPlan(local=tmp_path / "f.csv", remote="/r/f.csv")],
        retry_cfg=retry_cfg_fast,
        sleeper=lambda _: None,
    )
    argv = captured["argv"]
    assert argv[0] == "sftp"
    assert "-b" in argv
    assert "-i" in argv
    assert str(cfg.key_path) in argv
    assert "-P" in argv and "2222" in argv
    assert f"UserKnownHostsFile={cfg.known_hosts}" in argv
    assert "StrictHostKeyChecking=yes" in argv
    assert "BatchMode=yes" in argv
    assert f"{cfg.user}@{cfg.host}" in argv


def test_upload_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    calls = {"n": 0}

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] < 3:
            return MagicMock(returncode=1, stderr=b"transient")
        return MagicMock(returncode=0, stderr=b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    upload(
        _cfg(tmp_path),
        [UploadPlan(local=tmp_path / "f", remote="/r/f")],
        retry_cfg=retry_cfg_fast,
        sleeper=lambda _: None,
    )
    assert calls["n"] == 3


def test_upload_raises_after_exhausting_retries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    calls = {"n": 0}

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        calls["n"] += 1
        return MagicMock(returncode=1, stderr=b"permanent denial")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(SftpUploadError, match="permanent denial"):
        upload(
            _cfg(tmp_path),
            [UploadPlan(local=tmp_path / "f", remote="/r/f")],
            retry_cfg=retry_cfg_fast,
            sleeper=lambda _: None,
        )
    assert calls["n"] == retry_cfg_fast.max_attempts


def test_upload_timeout_raises_sftp_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        raise subprocess.TimeoutExpired(cmd=argv, timeout=0.1)

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(SftpUploadError, match="timed out"):
        upload(
            _cfg(tmp_path),
            [UploadPlan(local=tmp_path / "f", remote="/r/f")],
            retry_cfg=retry_cfg_fast,
            sleeper=lambda _: None,
            timeout=0.1,
        )


def test_upload_missing_sftp_binary_raises_sftp_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, retry_cfg_fast: RetryConfig,
) -> None:
    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        raise FileNotFoundError("no sftp")

    monkeypatch.setattr(subprocess, "run", fake_run)
    with pytest.raises(SftpUploadError, match="not found"):
        upload(
            _cfg(tmp_path),
            [UploadPlan(local=tmp_path / "f", remote="/r/f")],
            retry_cfg=retry_cfg_fast,
            sleeper=lambda _: None,
        )
```

▶️ **Run:** `pytest tests/test_sftp_uploader.py`

✅ **Checkpoint:** `6 passed` — *without any SFTP server*. Whole suite: `pytest` → **63 passed.
Cumulative: 63.** 🏁 **Phase D milestone:** validation and delivery are complete and fully unit-tested
offline.

💡 **Why.** **`subprocess.run(argv_list, ...)` with no `shell=True`** means the OS runs the binary
directly with those exact arguments — no shell parses them, so a filename containing `; rm -rf /` is just
a literal argument (the list form sidesteps shell injection entirely). The security contract is *pinned
as a test*: `test_upload_invokes_sftp_with_strict_host_checking` asserts the full hardened argv —
`StrictHostKeyChecking=yes` (defeats MITM), `BatchMode=yes` (never prompt — fail fast), key auth, and a
pinned `UserKnownHostsFile`. The remaining tests prove every failure mode maps to `SftpUploadError`
(non-zero exit, timeout, missing binary) and that retries actually happen on the configured schedule.

---

# PHASE E — Control plane & wiring

## ✅ E1 — Kafka control consumer

🎯 **Goal:** consume the control topic, decode each message into a `ControlMessage`, and hand the caller
a **manual-commit callback** — with poison/null handling — testable with no broker.

📦 **Depends on:** A3 (`KafkaConfig`), A2 (`ControlMessageError`, `ControlMessage`).

📨 **Kafka concept — offsets & commits.** A partition is an ordered log; each message has a monotonic
**offset**. Commits are **cumulative**: committing offset N means "everything ≤ N is done." We disabled
auto-commit (A3), so *we* commit — only after a job fully succeeds (at-least-once delivery).

📄 **Build — `src/etl/control_consumer.py`:**

```python
"""Thin wrapper around `confluent_kafka.Consumer` for the control topic.

* `enable.auto.commit=False` — offsets are only committed via the explicit
  ack callback returned with each message.
* Decodes message JSON into a `ControlMessage`. Malformed messages raise
  `ControlMessageError` so the orchestrator can decide whether to commit
  past the poison record or skip-and-alert.
"""

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
    def __init__(self, cfg: KafkaConfig, *, consumer_factory: Callable[[dict[str, str]], Any] | None = None) -> None:
        if consumer_factory is None:
            from confluent_kafka import Consumer  # local import — heavy dep
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
            raise ControlMessageError(f"control message must be a JSON object, got {type(payload).__name__}")
        job_doc_id = payload.get("job_doc_id") or payload.get("id")
        if not isinstance(job_doc_id, str) or not job_doc_id:
            raise ControlMessageError("control message missing required 'job_doc_id'")
        correlation_id = payload.get("correlation_id")
        if correlation_id is not None and not isinstance(correlation_id, str):
            raise ControlMessageError("'correlation_id' must be string or absent")
        return ControlMessage(
            job_doc_id=job_doc_id,
            correlation_id=correlation_id,
            raw_partition=partition,
            raw_offset=offset,
        )

    def iter_messages(
        self,
        *,
        poll_timeout_s: float = 1.0,
        stop: Callable[[], bool] | None = None,
    ) -> Iterator[tuple[ControlMessage, Callable[[], None], Any]]:
        """Yield `(ControlMessage, commit_fn, raw_kafka_msg)` tuples.

        `commit_fn()` commits the offset for that message synchronously. Call
        it only after the job has been processed successfully — failures
        should leave the offset unmoved so the message is redelivered.
        """
        while True:
            if stop is not None and stop():
                return
            msg = self._consumer.poll(timeout=poll_timeout_s)
            if msg is None:
                continue
            if msg.error():
                _log.warning("kafka poll error: %r", msg.error())
                continue
            # partition()/offset() are typed Optional but are always present on
            # a fetched record; coalesce to satisfy the type checker.
            partition = int(msg.partition() or 0)
            offset = int(msg.offset() or 0)
            value = msg.value()
            if value is None:
                # Null-valued record (e.g. a tombstone): nothing to act on.
                _log.warning("null control message value at p=%s o=%s; skipping",
                             partition, offset)
                self._consumer.commit(message=msg, asynchronous=False)
                continue
            raw = value.encode("utf-8") if isinstance(value, str) else value
            try:
                ctrl = self._decode(raw, partition=partition, offset=offset)
            except ControlMessageError as e:
                _log.error("poison control message at p=%s o=%s: %r", partition, offset, e)
                # Skip past it so the daemon doesn't loop on poison forever.
                self._consumer.commit(message=msg, asynchronous=False)
                continue

            def _commit(_m: Any = msg) -> None:
                self._consumer.commit(message=_m, asynchronous=False)

            yield ctrl, _commit, msg

    def close(self) -> None:
        try:
            self._consumer.close()
        except Exception as e:
            _log.warning("consumer close failed: %r", e)
```

🧪 **Test — `tests/test_control_consumer.py`** (a hand-written `FakeConsumer` via the injectable
factory; uses the `FakeKafkaMessage` fixture from conftest):

```python
from __future__ import annotations

import json
from typing import Any

from etl.config import KafkaConfig
from etl.control_consumer import ControlConsumer


class FakeConsumer:
    def __init__(self, cfg: dict[str, str]) -> None:
        self.cfg = cfg
        self.subscribed: list[str] = []
        self.committed: list[Any] = []
        self.queue: list[Any] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscribed = topics

    def poll(self, timeout: float) -> Any:
        if self.queue:
            return self.queue.pop(0)
        return None

    def commit(self, *, message: Any, asynchronous: bool) -> None:
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


def _cfg() -> KafkaConfig:
    return KafkaConfig(bootstrap_servers="b", control_topic="ctl", group_id="g")


def _msg(value: bytes, partition: int = 0, offset: int = 0, err: Any = None,
         FakeKafkaMessage: Any = None) -> Any:
    return FakeKafkaMessage(value, partition=partition, offset=offset, err=err)


def test_consumer_subscribes_with_manual_commit_config() -> None:
    fake_holder: list[FakeConsumer] = []

    def factory(cfg: dict[str, str]) -> FakeConsumer:
        c = FakeConsumer(cfg)
        fake_holder.append(c)
        return c

    cc = ControlConsumer(_cfg(), consumer_factory=factory)
    assert fake_holder[0].subscribed == ["ctl"]
    assert fake_holder[0].cfg["enable.auto.commit"] == "false"
    cc.close()
    assert fake_holder[0].closed is True


def test_consumer_decodes_valid_message_and_commit_only_after_ack(
    FakeKafkaMessage: Any,
) -> None:
    consumer_ref: dict[str, FakeConsumer] = {}

    def factory(cfg: dict[str, str]) -> FakeConsumer:
        c = FakeConsumer(cfg)
        consumer_ref["c"] = c
        return c

    cc = ControlConsumer(_cfg(), consumer_factory=factory)
    fake = consumer_ref["c"]
    fake.queue.append(_msg(json.dumps({"job_doc_id": "abc"}).encode("utf-8"),
                           partition=2, offset=99,
                           FakeKafkaMessage=FakeKafkaMessage))

    it = cc.iter_messages(poll_timeout_s=0.0,
                          stop=lambda: not fake.queue and True)
    # Force evaluation: fetch first message then stop the generator.
    ctrl, commit, raw = next(it)
    assert ctrl.job_doc_id == "abc"
    assert ctrl.raw_partition == 2
    assert ctrl.raw_offset == 99
    assert fake.committed == []
    commit()
    assert fake.committed == [raw]
    it.close()


def test_consumer_skips_null_value_message(FakeKafkaMessage: Any) -> None:
    consumer_ref: dict[str, FakeConsumer] = {}

    def factory(cfg: dict[str, str]) -> FakeConsumer:
        c = FakeConsumer(cfg)
        consumer_ref["c"] = c
        return c

    cc = ControlConsumer(_cfg(), consumer_factory=factory)
    fake = consumer_ref["c"]
    fake.queue.append(_msg(None, offset=3, FakeKafkaMessage=FakeKafkaMessage))  # type: ignore[arg-type]
    fake.queue.append(_msg(json.dumps({"job_doc_id": "ok"}).encode("utf-8"),
                           offset=4, FakeKafkaMessage=FakeKafkaMessage))

    it = cc.iter_messages(poll_timeout_s=0.0, stop=lambda: not fake.queue)
    ctrl, _commit, _ = next(it)
    assert ctrl.job_doc_id == "ok"
    # The null-valued record was committed past before the good one.
    assert len(fake.committed) == 1
    it.close()


def test_consumer_skips_poison_message_by_committing(FakeKafkaMessage: Any) -> None:
    consumer_ref: dict[str, FakeConsumer] = {}

    def factory(cfg: dict[str, str]) -> FakeConsumer:
        c = FakeConsumer(cfg)
        consumer_ref["c"] = c
        return c

    cc = ControlConsumer(_cfg(), consumer_factory=factory)
    fake = consumer_ref["c"]
    fake.queue.append(_msg(b"not-json", FakeKafkaMessage=FakeKafkaMessage))
    fake.queue.append(_msg(json.dumps({"job_doc_id": "ok"}).encode("utf-8"),
                           offset=5, FakeKafkaMessage=FakeKafkaMessage))

    it = cc.iter_messages(poll_timeout_s=0.0, stop=lambda: not fake.queue)
    ctrl, commit, _ = next(it)
    assert ctrl.job_doc_id == "ok"
    # The poison message was committed past (1 commit so far); ack the good one.
    assert len(fake.committed) == 1
    commit()
    assert len(fake.committed) == 2
    it.close()
```

▶️ **Run:** `pytest tests/test_control_consumer.py`

✅ **Checkpoint:** `4 passed`. Whole suite: `pytest` → **67 passed. Cumulative: 67.**

💡 **Why.** **DI via `consumer_factory`** is the seam that lets you test a Kafka consumer with no broker:
the class only needs "something constructible from a config dict with `poll/commit/subscribe/close`."
The **closure default-argument trick** — `def _commit(_m=msg):` — captures the *current* `msg` at
definition time; without `_m=msg` every commit in the loop would refer to the last message (the classic
late-binding bug). The generator yields the *commit action* rather than committing itself, pushing
"when to acknowledge" up to the orchestrator. The tests pin the three behaviors that keep a daemon
alive: commit-only-after-ack, skip-past-null, and skip-past-poison (so a bad record can't wedge the
loop forever).

---

## ✅ E2 — Pipeline orchestration

🎯 **Goal:** compose every stage into `run_one` — the function that processes one control message. It
deliberately does **not** commit (that's the daemon's call).

📦 **Depends on:** essentially all of A–D, plus C1's `tee_to_ndjson`.

📄 **Build — `src/etl/pipeline.py`:**

```python
"""End-to-end orchestration for one control message.

`run_one` is the single entry point. It does *not* commit offsets — the
caller (`__main__`) holds the commit decision so a failing job leaves the
offset unmoved (the control message is redelivered on the next poll).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from es_extract.diagnostics import tee_to_ndjson
from etl.config import Settings
from etl.csv_writer import write_csv
from etl.extractor import expected_count, iter_hits
from etl.job_loader import load_job
from etl.models import ControlMessage, CsvResult, JobSpec
from etl.sftp_uploader import UploadPlan, upload
from etl.transformer import iter_transformed
from etl.validator import validate_with_retry

_log = logging.getLogger(__name__)


def _staged_paths(csv_dir: Path, job: JobSpec) -> tuple[Path, str, str]:
    """Local staging path for the CSV + remote target paths for csv & sidecar."""
    local_basename = Path(job.remote_filename).name
    local_csv = csv_dir / local_basename
    remote_csv = job.remote_filename
    remote_sidecar = remote_csv + ".sha256"
    return local_csv, remote_csv, remote_sidecar


def _do_extract_to_csv(
    *,
    es: Any,
    job: JobSpec,
    page_size: int,
    keep_alive: str,
    local_csv: Path,
    raw_dump_path: Path | None = None,
) -> CsvResult:
    hits = iter_hits(es, job, page_size=page_size, keep_alive=keep_alive)
    if raw_dump_path is not None:
        # Diagnostic: tee the raw extracted hits to NDJSON as they stream.
        hits = tee_to_ndjson(hits, raw_dump_path)
    rows = iter_transformed(hits, job.column_paths, job.columns, job_id=job.job_id)
    return write_csv(rows, job.columns, local_csv)


def run_one(
    *,
    ctrl: ControlMessage,
    es: Any,
    settings: Settings,
) -> None:
    log_extra = {
        "job_doc_id": ctrl.job_doc_id,
        "correlation_id": ctrl.correlation_id,
        "kafka_partition": ctrl.raw_partition,
        "kafka_offset": ctrl.raw_offset,
    }
    _log.info("loading job", extra=log_extra)
    job = load_job(es, job_index=settings.es.job_index, job_doc_id=ctrl.job_doc_id)
    log_extra["job_id"] = job.job_id
    log_extra["data_index"] = job.data_index

    initial_count = expected_count(es, job.data_index, job.query)
    _log.info("expected_count=%d", initial_count, extra=log_extra)

    local_csv, remote_csv, remote_sidecar = _staged_paths(settings.csv_output_dir, job)

    raw_dump_path = (
        settings.raw_dump_dir / f"{job.job_id}.ndjson"
        if settings.raw_dump_dir is not None
        else None
    )
    if raw_dump_path is not None:
        _log.info("raw hit dump enabled path=%s", raw_dump_path, extra=log_extra)

    csv_result = _do_extract_to_csv(
        es=es,
        job=job,
        page_size=settings.pagination.page_size,
        keep_alive=settings.pagination.pit_keep_alive,
        local_csv=local_csv,
        raw_dump_path=raw_dump_path,
    )
    _log.info("csv written rows=%d sha256=%s",
              csv_result.row_count, csv_result.sha256_hex, extra=log_extra)

    def _reextract() -> CsvResult:
        _log.warning("re-extracting after count mismatch", extra=log_extra)
        # A fresh call opens a new point-in-time; the previous one is spent.
        return _do_extract_to_csv(
            es=es, job=job,
            page_size=settings.pagination.page_size,
            keep_alive=settings.pagination.pit_keep_alive,
            local_csv=local_csv,
            raw_dump_path=raw_dump_path,
        )

    csv_result = validate_with_retry(
        es=es,
        index=job.data_index,
        query=job.query,
        csv_result=csv_result,
        retry_cfg=settings.retry,
        on_full_reextract=_reextract,
        log_extra=log_extra,
    )
    _log.info("counts validated rows=%d", csv_result.row_count, extra=log_extra)

    upload(
        settings.sftp,
        [
            UploadPlan(local=csv_result.csv_path, remote=remote_csv),
            UploadPlan(local=csv_result.sidecar_path, remote=remote_sidecar),
        ],
        retry_cfg=settings.retry,
    )
    _log.info("upload complete remote=%s", remote_csv, extra=log_extra)
```

🧪 **Test — `tests/test_pipeline.py`** (end-to-end with a fully faked `es` and a monkeypatched
`subprocess.run`; uses the `settings` fixture):

```python
from __future__ import annotations

import dataclasses
import json
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from etl.config import Settings
from etl.errors import RecordCountMismatch, SftpUploadError
from etl.models import ControlMessage
from etl.pipeline import run_one


def _ctrl() -> ControlMessage:
    return ControlMessage(job_doc_id="doc-1", correlation_id="corr-1",
                          raw_partition=0, raw_offset=10)


def _es_with_job_and_hits(hits: list[dict[str, Any]]) -> MagicMock:
    es = MagicMock()
    es.get.return_value = {
        "found": True,
        "_source": {
            "job_id": "job-1",
            "data_index": "data",
            "query": {"match_all": {}},
            "column_paths": {},
            "columns": ["id", "name"],
            "remote_filename": "exports/job-1.csv",
        },
    }
    es.count.return_value = {"count": len(hits)}
    es.open_point_in_time.return_value = {"id": "pit-1"}

    # PIT + search_after paging. Every extraction opens a fresh PIT and starts
    # with no `search_after`: return the full page first, then an empty page so
    # iteration terminates. Keyed on the body so repeated extractions (the
    # re-extract path) work too.
    page = {
        "pit_id": "pit-1",
        "hits": {"hits": [{"_source": h, "sort": [i]} for i, h in enumerate(hits)]},
    }
    empty: dict[str, Any] = {"pit_id": "pit-1", "hits": {"hits": []}}

    def _search(**kwargs: Any) -> dict[str, Any]:
        return empty if "search_after" in kwargs["body"] else page

    es.search.side_effect = _search
    return es


def test_pipeline_golden_path(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, tmp_path: Path,
) -> None:
    hits = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    es = _es_with_job_and_hits(hits)
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: MagicMock(returncode=0, stderr=b""))

    run_one(ctrl=_ctrl(), es=es, settings=settings)

    staged = settings.csv_output_dir / "job-1.csv"
    sidecar = settings.csv_output_dir / "job-1.csv.sha256"
    assert staged.exists()
    assert sidecar.exists()
    # 2 data rows + header
    assert len(staged.read_text(encoding="utf-8").splitlines()) == 3


def test_pipeline_count_mismatch_after_reextract_raises(
    monkeypatch: pytest.MonkeyPatch, settings: Settings,
) -> None:
    hits = [{"id": "1", "name": "Alice"}]
    es = _es_with_job_and_hits(hits)
    # ES count always disagrees with row count (1).
    es.count.return_value = {"count": 99}

    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: MagicMock(returncode=0, stderr=b""))

    with pytest.raises(RecordCountMismatch):
        run_one(ctrl=_ctrl(), es=es, settings=settings)
    # SFTP should never be invoked.


def test_pipeline_sftp_failure_after_retries(
    monkeypatch: pytest.MonkeyPatch, settings: Settings,
) -> None:
    hits = [{"id": "1", "name": "Alice"}]
    es = _es_with_job_and_hits(hits)
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: MagicMock(returncode=1, stderr=b"denied"))

    with pytest.raises(SftpUploadError):
        run_one(ctrl=_ctrl(), es=es, settings=settings)


def test_pipeline_transient_sftp_recovers(
    monkeypatch: pytest.MonkeyPatch, settings: Settings,
) -> None:
    hits = [{"id": "1", "name": "Alice"}]
    es = _es_with_job_and_hits(hits)
    calls = {"n": 0}

    def fake_run(argv: list[str], **kwargs: Any) -> Any:
        calls["n"] += 1
        if calls["n"] < 3:
            return MagicMock(returncode=1, stderr=b"transient")
        return MagicMock(returncode=0, stderr=b"")

    monkeypatch.setattr(subprocess, "run", fake_run)
    run_one(ctrl=_ctrl(), es=es, settings=settings)
    assert calls["n"] == 3


def test_pipeline_raw_dump_writes_ndjson(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, tmp_path: Path,
) -> None:
    hits = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    es = _es_with_job_and_hits(hits)
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: MagicMock(returncode=0, stderr=b""))

    dump_dir = tmp_path / "raw"
    s = dataclasses.replace(settings, raw_dump_dir=dump_dir)
    run_one(ctrl=_ctrl(), es=es, settings=s)

    dump = dump_dir / "job-1.ndjson"
    assert dump.exists()
    lines = dump.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line) for line in lines] == hits  # raw _source hits, one per line
```

▶️ **Run:** `pytest tests/test_pipeline.py`

✅ **Checkpoint:** `5 passed`. Whole suite: `pytest` → **72 passed. Cumulative: 72.**

💡 **Why.** `run_one` is **composition**: each stage is a function; `run_one` wires inputs to outputs.
The lazy chain `iter_hits → (tee) → iter_transformed → write_csv` doesn't *run* until `write_csv` pulls
it — one row at a time. The `_reextract` **closure** captures `es`/`job`/`settings`/paths and hands the
validator a zero-arg callable that opens a *fresh* PIT each call. Because `run_one` takes `es` and
`settings` as parameters (DI), the whole pipeline is testable with fakes — the golden-path test is the
unit-level mirror of the Phase G live smoke run. The clever fake here: `search.side_effect` is a
*function* keyed on the body, so it terminates correctly *and* works for the re-extract path.

---

## ✅ E3 — Daemon entry point

🎯 **Goal:** the long-lived process: poll, run a job, commit on success, **halt on failure**, shut down
gracefully on signals.

📦 **Depends on:** A3, A2, A4, E1, E2.

📄 **Build — `src/etl/__main__.py`:**

```python
"""Daemon entry point — `python -m etl`.

Polls the control topic in a loop, runs one job per message, and commits the
offset only after the job succeeds. Job-scoped errors (`EtlError`) are
logged and the loop continues. Anything else propagates and exits non-zero.
"""

from __future__ import annotations

import contextlib
import logging
import signal
import sys
from types import FrameType
from typing import Any

from etl.config import Settings, load_settings
from etl.control_consumer import ControlConsumer
from etl.errors import ConfigError, EtlError
from etl.logging_setup import configure_logging
from etl.pipeline import run_one

_log = logging.getLogger("etl.main")


def _build_es_client(settings: Settings) -> Any:
    """Construct the official `elasticsearch.Elasticsearch` client."""
    from elasticsearch import Elasticsearch  # local import — heavy dep

    kwargs: dict[str, Any] = {"hosts": settings.es.hosts}
    if settings.es.api_key:
        kwargs["api_key"] = settings.es.api_key
    elif settings.es.username and settings.es.password:
        kwargs["basic_auth"] = (settings.es.username, settings.es.password)
    return Elasticsearch(**kwargs)


def main() -> int:
    try:
        settings = load_settings()
    except ConfigError as e:
        # Logging may not be configured yet; print and exit.
        print(f"config error: {e}", file=sys.stderr)
        return 2

    configure_logging(settings.log_level)
    _log.info("starting etl daemon", extra={"control_topic": settings.kafka.control_topic})

    stopping = {"flag": False}

    def _on_signal(signum: int, _frame: FrameType | None) -> None:
        _log.info("received signal %s; stopping", signum)
        stopping["flag"] = True

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
                # The job already exhausted its internal retries (counts, SFTP)
                # before raising. Halt without committing so the offset stays
                # put and this exact message is redelivered on the next start.
                # We must NOT `continue`: advancing to the next message and
                # committing its offset would commit *over* this failed one
                # (Kafka offsets are "up to and including"), silently dropping it.
                _log.error(
                    "job failed; halting without commit so it is redelivered: %r",
                    e,
                    extra={
                        "job_doc_id": ctrl.job_doc_id,
                        "correlation_id": ctrl.correlation_id,
                    },
                )
                exit_code = 1
                break
            commit()
            _log.info("job committed", extra={"job_doc_id": ctrl.job_doc_id})
    finally:
        consumer.close()
        with contextlib.suppress(Exception):  # pragma: no cover - best effort
            es.close()

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
```

🧪 **Test / smoke:** the loop is integration-shaped, so it's covered by the Phase G smoke test rather
than a unit test (its logic lives in `run_one` + `ControlConsumer`, both already tested). You can still
verify the wiring without infra by exercising the **config-error exit path**:

```bash
# With no env set, load_settings() raises ConfigError → prints to stderr, exits 2.
env -i .venv/bin/python -m etl ; echo "exit=$?"
```

✅ **Checkpoint:** prints `config error: missing required env var: KAFKA_BOOTSTRAP_SERVERS` and
`exit=2`. (With a full `.env`, `python -m etl` instead logs `starting etl daemon` and blocks on poll.)
Whole suite is still **72 passed. Cumulative: 72.** 🏁 **Phase E milestone:** the daemon is wired
end-to-end.

💡 **Why — the subtle correctness bug, made explicit.** Commits are cumulative. If message 5 fails and
you `continue` (no commit), then message 6 succeeds and you commit 6, you've committed *over* 5 and
**silently lost the failed job**. So on a hard failure (internal retries already exhausted) the daemon
**`break`s without committing** — the offset stays put and the message is redelivered on restart. Signal
handlers set a flag the loop checks between jobs (never abort mid-job). Distinct exit codes (`2` config,
`1` job failure, `0` clean) let an orchestrator react.

---

# PHASE F — The full suite & the intentional red bar

You now have **72 green tests** across 13 modules. This phase does two things: confirm the whole suite
is green, then add the *adversarial* test file — which is **deliberately red**.

▶️ **First, the all-green proof (everything except the adversarial probes):**

```bash
pytest --ignore=tests/test_adversarial.py
```

✅ **Checkpoint:** **72 passed.** Every checkpoint you locked in is still green — no regressions.

### The adversarial probes — documentation-as-tests

The build's final test file flips the usual polarity. Each probe asserts the behavior a *careful user
would expect*; a **failure here reveals a real defect or unguarded boundary**, not a flaky test. Of 12
probes, **3 pass** (those paths are robust) and **9 fail by design** — each failure is a checkpoint that
a known gap still exists.

🧪 **Add — `tests/test_adversarial.py`:**

```python
"""Adversarial / unexpected-input probes.

Goal (per request): *try to break the suite* by feeding edge-case inputs and
asserting the behaviour a careful user would expect. Tests that FAIL here are
revealing a real defect or an unguarded boundary, not a flaky test. Tests that
PASS show the code already handles that input.

Each test's docstring states the expectation and (where relevant) the matching
REVIEW.md finding.
"""

from __future__ import annotations

import hashlib
import io
import json
import random
from typing import Any

import pytest

from etl.config import load_settings
from etl.csv_writer import _HashingWriter, _stringify, write_csv
from etl.errors import ConfigError, JobSpecError
from etl.job_loader import load_job
from etl.retry import _compute_delay
from etl.transformer import get_by_path, iter_transformed, project

_BASE_ENV: dict[str, str] = {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "KAFKA_CONTROL_TOPIC": "ctl",
    "KAFKA_GROUP_ID": "g",
    "ES_HOSTS": "http://localhost:9200",
    "ES_JOB_INDEX": "jobs",
    "SFTP_HOST": "h",
    "SFTP_USER": "u",
    "SFTP_KEY_PATH": "/tmp/key",
    "SFTP_KNOWN_HOSTS": "/tmp/kh",
}


def _set_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    for k in list(_BASE_ENV) + list(overrides):
        monkeypatch.delenv(k, raising=False)
    for k, v in {**_BASE_ENV, **overrides}.items():
        monkeypatch.setenv(k, v)


class _FakeES:
    """Minimal ES double returning a fixed job document from .get()."""

    def __init__(self, source: dict[str, Any]) -> None:
        self._source = source

    def get(self, *, index: str, id: str) -> dict[str, Any]:  # noqa: A002
        return {"found": True, "_source": self._source}


# 1. CSV writer / hashing

def test_hashing_writer_returns_bytes_written() -> None:
    """`write` should report the number of *bytes* written (io contract)."""
    h = hashlib.sha256()
    w = _HashingWriter(io.BytesIO(), h)
    s = "café"  # 4 chars, 5 UTF-8 bytes
    assert w.write(s) == len(s.encode("utf-8"))


def test_csv_formula_injection_is_neutralized(tmp_path: Any) -> None:
    """A cell beginning with '=' is a spreadsheet formula-injection vector."""
    out = tmp_path / "f.csv"
    write_csv([{"name": "=1+1"}], ["name"], out)
    body_cell = out.read_text().splitlines()[1]
    assert not body_cell.startswith("="), f"unescaped formula written: {body_cell!r}"


def test_nested_value_serializes_as_json() -> None:
    """A column whose path resolves to an object should become valid JSON."""
    assert _stringify({"a": 1}) == json.dumps({"a": 1})


def test_csv_unicode_hash_round_trips(tmp_path: Any) -> None:
    """Robustness check: multibyte content still hashes consistently."""
    out = tmp_path / "u.csv"
    res = write_csv([{"name": "naïve 😀 café"}], ["name"], out)
    assert res.sha256_hex == hashlib.sha256(out.read_bytes()).hexdigest()


# 2. Config range checks

def test_zero_page_size_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """PAGE_SIZE=0 is nonsensical (a page must hold >=1 row)."""
    _set_env(monkeypatch, PAGE_SIZE="0")
    with pytest.raises(ConfigError):
        load_settings()


def test_negative_page_size_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """PAGE_SIZE=-5 is invalid input."""
    _set_env(monkeypatch, PAGE_SIZE="-5")
    with pytest.raises(ConfigError):
        load_settings()


# 3. Job loader boundary validation

def test_empty_columns_rejected() -> None:
    """A job with no columns can never produce a CSV."""
    es = _FakeES(
        {
            "data_index": "d",
            "query": {"match_all": {}},
            "columns": [],
            "remote_filename": "out.csv",
        }
    )
    with pytest.raises(JobSpecError):
        load_job(es, job_index="jobs", job_doc_id="j1")


def test_non_dict_query_rejected() -> None:
    """Robustness check: a non-object `query` is rejected at the boundary."""
    es = _FakeES(
        {
            "data_index": "d",
            "query": ["not", "a", "dict"],
            "columns": ["a"],
            "remote_filename": "out.csv",
        }
    )
    with pytest.raises(JobSpecError):
        load_job(es, job_index="jobs", job_doc_id="j1")


# 4. Transformer path edge cases

def test_dot_only_path_does_not_leak_whole_document() -> None:
    """A path of just "." tokenizes to nothing → should yield ""."""
    doc = {"secret": "leak-me", "nested": {"k": "v"}}
    assert get_by_path(doc, ".") == ""


def test_document_id_is_projectable() -> None:
    """The ES `_id` is the natural primary key for many exports."""
    source_hit = {"order_id": "o-1", "customer": {"name": "Acme"}}
    rows = list(
        iter_transformed(
            [source_hit],
            {"id": "_id", "cust": "customer.name"},
            ["id", "cust"],
            job_id="j",
        )
    )
    assert rows[0]["id"] != "", "document _id should be projectable (REVIEW §3.1)"


def test_huge_list_index_is_safe() -> None:
    """Robustness check: an out-of-range list index degrades to ""."""
    assert get_by_path({"items": [{"sku": "s"}]}, "items[999999999].sku") == ""


# 5. Retry backoff arithmetic

def test_compute_delay_does_not_overflow_at_high_attempt() -> None:
    """Backoff must stay bounded by `cap` for any attempt number."""
    d = _compute_delay(attempt=2000, base=1.0, cap=30.0, jitter=0.0, rng=random.Random(0))
    assert d == 30.0
```

▶️ **Run the whole suite:** `pytest`

✅ **Checkpoint:** **75 passed, 9 failed.** The 9 failures are *expected* and map one-to-one to the
inline ⚠️ warnings you saw while building:

| Failing probe | Gap it documents | Built at |
|---|---|---|
| `test_hashing_writer_returns_bytes_written` | `_HashingWriter.write` returns char count, not bytes | B2 |
| `test_csv_formula_injection_is_neutralized` | `=1+1` written raw (CSV injection) | B2 |
| `test_nested_value_serializes_as_json` | object cell becomes Python `repr`, not JSON | B2 |
| `test_zero_page_size_rejected` | `PAGE_SIZE=0` accepted (no range check) | A3 |
| `test_negative_page_size_rejected` | `PAGE_SIZE=-5` accepted | A3 |
| `test_empty_columns_rejected` | `columns: []` accepted at the boundary | C3 |
| `test_dot_only_path_does_not_leak_whole_document` | `"."` returns the whole document | B1 |
| `test_document_id_is_projectable` | `_id` unreachable after `source_only` reduction | C1/C2 |
| `test_compute_delay_does_not_overflow_at_high_attempt` | backoff overflows before the cap | A5 |

The 3 passing probes (`...unicode_hash_round_trips`, `...non_dict_query_rejected`,
`...huge_list_index_is_safe`) confirm those paths are already robust.

💡 **Why keep a red bar?** This *is* the checkpoint philosophy taken to its logical end: a failing test
is the most precise possible description of a known gap. Two ways to a clean green bar when you're ready:

1. **Fix the 9 defects** (the satisfying path) — e.g. clamp `_compute_delay` with `min(attempt, ceiling)`,
   range-check `PAGE_SIZE`, reject empty `columns` in `load_job`, neutralize leading-`=` cells, return
   `len(b)` from `_HashingWriter.write`, JSON-encode object cells, guard the `"."` path, and surface
   `_id` (extract with `source_only=False`). Each fix flips exactly one probe to green — a checkpoint in
   its own right.
2. **Mark them `xfail`** with a reason (`@pytest.mark.xfail(reason="REVIEW §3.x — deferred")`) so the
   suite is green *and* the known gaps stay documented and visible in the report.

See [`REVIEW.v3.md`](REVIEW.v3.md) §D for the consolidated findings and recommended fix order.

---

# PHASE G — Local stack & the live smoke test

The unit suite proves every module in isolation. This final phase proves them *together* against real
(disposable) infrastructure.

📄 **Provide** (full files are in the repo — they're plumbing, not application logic):
`docker-compose.yml` (Kafka in KRaft mode, single-node ES with security off, `atmoz/sftp`),
`scripts/setup_local.sh` (generates SSH keys + host keys, captures `known_hosts`, creates the control
topic), `scripts/teardown_local.sh`, and `scripts/seed.py` (writes a job doc + sample data + one control
message).

▶️ **The end-to-end smoke test (manual):**

```bash
./scripts/setup_local.sh
cp .env.local .env
.venv/bin/python scripts/seed.py
.venv/bin/python -m etl          # processes the one control message, then blocks
# in another shell, or after Ctrl-C once you see "upload complete":
ls local/sftp/upload/            # CSV + .sha256 delivered
sha256sum -c local/sftp/upload/*.sha256
```

✅ **Checkpoint / 🏁 PROJECT MILESTONE:** the delivered CSV's checksum **verifies (`OK`)**, and the daemon
log shows the trace `loading job → expected_count → csv written → counts validated → upload complete →
job committed`.

**Failure-mode drills — prove the safety nets you tested in units actually fire end-to-end:**

| Drill | How | Expected |
|---|---|---|
| Count mismatch | delete a data doc before starting | 5 count retries + 1 re-extract → `RecordCountMismatch`; offset **not** committed |
| SFTP down | `docker stop etl-sftp` | retries with backoff → `SftpUploadError`; daemon halts (exit 1) |
| Redelivery | Ctrl-C mid-job, restart | the same message is reprocessed (offset stayed put) |

When all three behave as described, the service is **complete and demonstrably correct end-to-end.**

---

# Appendix A — The checkpoint ledger, recapped

```
A1 scaffolding ............ 0   (harness runs)
A2 errors+models ......... +2  → 2
A3 config+conftest ....... +4  → 6
A4 logging ............... +1  → 7
A5 retry ................. +8  → 15   🏁 Phase A
B1 transformer ........... +9  → 24
B2 csv_writer ............ +5  → 29   🏁 Phase B (Stage 0: offline CSV)
C1 es_extract ........... +11  → 40
C2 extractor ............. +4  → 44
C3 job_loader ............ +7  → 51   🏁 Phase C
D1 validator ............. +6  → 57
D2 sftp_uploader ......... +6  → 63   🏁 Phase D
E1 control_consumer ...... +4  → 67
E2 pipeline .............. +5  → 72
E3 daemon ................ +0  → 72   🏁 Phase E
F  adversarial ........... 75 pass / 9 fail-by-design
G  local smoke ........... delivered + 3 drills  🏁 PROJECT
```

**Build order rule of thumb:** a module's checkpoint can only be attempted once every checkpoint in its
*📦 Depends on* line is green. Errors/models/config/logging/retry underpin everything; the transform
core needs only those; ES needs the core; trust & delivery need ES; wiring needs all of it.

# Appendix B — How this maps to the other docs

- **Per-line annotation** of any module above: [`TUTORIAL.v3.md`](TUTORIAL.v3.md) (same code, read
  line by line).
- **Why** each decision was made: [`DESIGN.md`](DESIGN.md).
- **Findings & the fix list** behind Phase F's red bar: [`REVIEW.v3.md`](REVIEW.v3.md) §D.
- **Earlier phased build logs** (abridged tests): [`TUTORIAL.md`](TUTORIAL.md) /
  [`TUTORIAL.v2.md`](TUTORIAL.v2.md).

# Appendix C — Concept index (where each idea is taught)

- **Python:** `from __future__ import annotations` & frozen dataclasses (A2) · exception chaining
  `raise … from e` and hidden-input hermeticity (A3) · structured logging via `record.__dict__` (A4) ·
  generics/`TypeVar`, decorators, **injectable clock**, `except tuple` (A5) · generators &
  `Iterable`/`Iterator`, defensive `isinstance` guards (B1) · duck typing + decorator-wrapper, context
  managers, one-pass hashing (B2) · resource-owning generators with `finally`, **injectable
  `error_cls`** (C1) · validate-at-the-boundary, generator-expression validation (C3) · inversion of
  control via callback (D1) · `subprocess` without `shell=True`, `shlex.quote`, `tempfile` (D2) ·
  closure default-arg capture, **DI factory** (E1) · composition + closures (E2) · signals,
  `contextlib.suppress`, cumulative-commit correctness (E3) · documentation-as-tests (F).
- **Kafka:** consumer config keys (A3) · offsets, cumulative commits, at-least-once (E1) ·
  halt-vs-continue offset-loss (E3) · KRaft local mode (G).
- **Elasticsearch:** deep-pagination limits, PIT + `search_after` (`keep_alive`, `_shard_doc`,
  `track_total_hits`, the `search_after` cursor) (C1) · `es.get` by id (C3) · response shape
  `hits.hits[]._source` (C1) · near-real-time refresh races (D1).
