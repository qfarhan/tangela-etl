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


# --------------------------------------------------------------------------
# 1. CSV writer / hashing
# --------------------------------------------------------------------------

def test_hashing_writer_returns_bytes_written() -> None:
    """`write` should report the number of *bytes* written (io contract).

    Expectation: a 4-char string that is 5 bytes in UTF-8 returns 5.
    (REVIEW §3.4 — the code returns len(s), the character count.)
    """
    h = hashlib.sha256()
    w = _HashingWriter(io.BytesIO(), h)
    s = "café"  # 4 chars, 5 UTF-8 bytes
    assert w.write(s) == len(s.encode("utf-8"))


def test_csv_formula_injection_is_neutralized(tmp_path: Any) -> None:
    """A cell beginning with '=' is a spreadsheet formula-injection vector.

    Expectation: the writer neutralizes it (e.g. prefixes a quote) so the
    emitted cell does not start with '='. (No CSV-injection guarding exists.)
    """
    out = tmp_path / "f.csv"
    write_csv([{"name": "=1+1"}], ["name"], out)
    body_cell = out.read_text().splitlines()[1]
    assert not body_cell.startswith("="), f"unescaped formula written: {body_cell!r}"


def test_nested_value_serializes_as_json() -> None:
    """A column whose path resolves to an object should become valid JSON.

    Expectation: '{"a": 1}'. (REVIEW §3.5 — `_stringify` returns Python repr
    `{'a': 1}` with single quotes, which is not JSON.)
    """
    assert _stringify({"a": 1}) == json.dumps({"a": 1})


def test_csv_unicode_hash_round_trips(tmp_path: Any) -> None:
    """Robustness check: multibyte content still hashes consistently."""
    out = tmp_path / "u.csv"
    res = write_csv([{"name": "naïve 😀 café"}], ["name"], out)
    assert res.sha256_hex == hashlib.sha256(out.read_bytes()).hexdigest()


# --------------------------------------------------------------------------
# 2. Config range checks
# --------------------------------------------------------------------------

def test_zero_page_size_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """PAGE_SIZE=0 is nonsensical (a page must hold >=1 row).

    Expectation: ConfigError at load time. (Only int-parsing is checked.)
    """
    _set_env(monkeypatch, PAGE_SIZE="0")
    with pytest.raises(ConfigError):
        load_settings()


def test_negative_page_size_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """PAGE_SIZE=-5 is invalid input.

    Expectation: ConfigError at load time. (No range validation exists.)
    """
    _set_env(monkeypatch, PAGE_SIZE="-5")
    with pytest.raises(ConfigError):
        load_settings()


# --------------------------------------------------------------------------
# 3. Job loader boundary validation
# --------------------------------------------------------------------------

def test_empty_columns_rejected() -> None:
    """A job with no columns can never produce a CSV.

    Expectation: load_job rejects `columns: []` at the boundary. (It accepts
    it — `all(isinstance(...))` is vacuously true — and the failure surfaces
    much later in write_csv.)
    """
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


# --------------------------------------------------------------------------
# 4. Transformer path edge cases
# --------------------------------------------------------------------------

def test_dot_only_path_does_not_leak_whole_document() -> None:
    """A path of just "." tokenizes to nothing.

    Expectation: a missing/degenerate path yields "" (like every other
    unresolved path). Instead get_by_path returns the *entire document*, which
    would then be stringified into a single cell.
    """
    doc = {"secret": "leak-me", "nested": {"k": "v"}}
    assert get_by_path(doc, ".") == ""


def test_document_id_is_projectable() -> None:
    """The ES `_id` is the natural primary key for many exports.

    Expectation: a column mapped to "_id" carries the id. After PIT +
    search_after, each hit is reduced to its `_source` (no envelope), so "_id"
    resolves to "". (REVIEW §3.1 / T5 — a genuine capability gap.)
    """
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


# --------------------------------------------------------------------------
# 5. Retry backoff arithmetic
# --------------------------------------------------------------------------

def test_compute_delay_does_not_overflow_at_high_attempt() -> None:
    """Backoff must stay bounded by `cap` for any attempt number.

    Expectation: the delay is capped at 30.0. The code computes
    `base * (2 ** attempt)` *before* applying the cap, so a large attempt
    overflows float conversion (OverflowError) instead of being clamped.
    """
    d = _compute_delay(attempt=2000, base=1.0, cap=30.0, jitter=0.0, rng=random.Random(0))
    assert d == 30.0
