# Build-It-Yourself Tutorial — v3 (Line-by-Line)

**What this is.** A *line-by-line* reading of the project's source. Where `TUTORIAL.md` (the build
log) tells you **what to build in what order** and `DESIGN.md` explains **why**, this document walks
**every line of every source module** and says what it does and why it's written that way. Read it with
the file open beside you.

**Scope.** This v3 covers, line by line:
- the standalone **`src/es_extract/`** package (5 modules), then
- the **`src/etl/`** application (15 modules), then
- the **packaging & config** files (`pyproject.toml`, `requirements*.txt`, `.env.example`, `.gitignore`).

The **tests** (`tests/`) and **operational scripts** (`scripts/`) are walked through *structurally* in
[`REVIEW.v3.md`](REVIEW.v3.md) §B.4–B.5; ask if you want those annotated line-by-line too.

**How to read each section.** For each module you get: a one-line summary, the full source in a fenced
block **with the real line numbers**, then a **Line-by-line** list. Runs of trivial lines (blanks,
plain imports) are grouped; every substantive line gets its own note.

**Conventions used throughout the code (worth internalizing once):**
- `from __future__ import annotations` is at the top of every module so all annotations are *strings*
  (PEP 563) — you can reference types without import-order pain and pay no runtime cost.
- Public functions are fully type-annotated; the project is `mypy --strict` clean.
- "Duck-typed `es`" means any object exposing the few ES methods used — the real client in production,
  a `MagicMock`/fake in tests.

---

## Table of contents

- **Part I — `es_extract` (the reusable extractor)**
  1. `errors.py` · 2. `diagnostics.py` · 3. `extract.py` · 4. `pagination.py` · 5. `__init__.py`
- **Part II — `etl` (the application)**
  6. `__init__.py` · 7. `errors.py` · 8. `models.py` · 9. `logging_setup.py` · 10. `config.py`
  · 11. `transformer.py` · 12. `csv_writer.py` · 13. `retry.py` · 14. `validator.py`
  · 15. `extractor.py` · 16. `job_loader.py` · 17. `control_consumer.py` · 18. `sftp_uploader.py`
  · 19. `pipeline.py` · 20. `__main__.py`
- **Part III — packaging & config**
  21. `pyproject.toml` · 22. `requirements.txt` / `requirements-dev.txt` · 23. `.env.example` · 24. `.gitignore`

---

# Part I — `es_extract` (the reusable extractor)

This package depends on **only** the standard library and a duck-typed Elasticsearch client. It has no
knowledge of Kafka, CSV, SFTP, or this repo's error types — which is exactly what makes it reusable.

## 1. `src/es_extract/errors.py`

The package's one exception type, plus the philosophy that keeps the package decoupled.

```python
 1  """Error type for the standalone ES-extraction package.
 2
 3  The functions and strategies here let the *caller* inject which exception type
 4  wraps a failure (`error_cls`), defaulting to `EsExtractError`. That keeps this
 5  package free of any dependency on a host application's error hierarchy: a host
 6  (like this repo's ``etl`` package) can pass its own ``ElasticsearchQueryError``
 7  so failures land in its existing ``except`` boundary, while a standalone user
 8  gets a plain ``EsExtractError``.
 9  """
10
11  from __future__ import annotations
12
13
14  class EsExtractError(Exception):
15      """Raised on an Elasticsearch request failure during extraction."""
```

**Line-by-line:**
- **Lines 1–9** — module docstring. It states the key design idea: callers inject the wrapping
  exception via an `error_cls` parameter, so this package never imports a host's error classes.
- **Line 11** — `from __future__ import annotations` (the project-wide convention above).
- **Line 14** — `EsExtractError` subclasses the built-in `Exception` directly. It deliberately does
  **not** inherit from anything in `etl` — that would create the dependency this package avoids.
- **Line 15** — a docstring is the entire body; the class needs no fields. It exists purely as a
  *type* to `raise`/`except`.

## 2. `src/es_extract/diagnostics.py`

A streaming "tee" that records the raw hit stream to NDJSON without buffering it.

```python
 1  """Capture the raw hit stream to disk for diagnostics — without buffering it.
 2
 3  ``tee_to_ndjson`` wraps any hit iterator: it yields each hit through unchanged
 4  while appending it as one JSON object per line (NDJSON). Because it is a
 5  generator that holds the file open for its own lifetime, it stays
 6  memory-bounded (one hit at a time) and the file is flushed/closed when
 7  iteration finishes *or* the consumer abandons it. Drop it into a pipeline to
 8  record exactly what flowed through.
 9  """
10
11  from __future__ import annotations
12
13  import json
14  from collections.abc import Iterable, Iterator
15  from pathlib import Path
16  from typing import Any
17
18
19  def tee_to_ndjson(
20      hits: Iterable[dict[str, Any]], path: Path
21  ) -> Iterator[dict[str, Any]]:
22      """Yield each hit unchanged while writing it as NDJSON to ``path``."""
23      path.parent.mkdir(parents=True, exist_ok=True)
24      with path.open("w", encoding="utf-8") as fh:
25          for hit in hits:
26              fh.write(json.dumps(hit, ensure_ascii=False, default=str))
27              fh.write("\n")
28              yield hit
29
30
31  def dump_to_ndjson(hits: Iterable[dict[str, Any]], path: Path) -> int:
32      """Eagerly write every hit to ``path`` as NDJSON; return the count written."""
33      written = 0
34      for _ in tee_to_ndjson(hits, path):
35          written += 1
36      return written
```

**Line-by-line:**
- **Lines 1–9** — docstring: the tee is a *generator* whose open file is bound to the generator's
  lifetime, so it's memory-bounded and self-closing.
- **Lines 13–16** — imports: `json` to serialize, `Iterable`/`Iterator` for precise stream typing,
  `Path` for the filesystem target, `Any` for the heterogeneous hit dicts.
- **Lines 19–21** — `tee_to_ndjson` signature: takes any `Iterable` of hit dicts and a `Path`, returns
  an `Iterator` (it's a generator). Same in/out element type → it's a pass-through.
- **Line 23** — ensure the parent directory exists (`parents=True, exist_ok=True` makes it idempotent
  and safe if the dir already exists).
- **Line 24** — `with path.open(...)` opens the file for the generator's lifetime. The `with` block
  spans the whole `for` loop, so the file flushes/closes when iteration ends **or** when the consumer
  stops pulling and Python finalizes the generator. `encoding="utf-8"` pins the encoding.
- **Line 25** — iterate the upstream hits lazily — we never hold more than one.
- **Line 26** — serialize the hit to one JSON line. `ensure_ascii=False` keeps real Unicode (not
  `\uXXXX` escapes); `default=str` makes non-JSON-native values (e.g. dates) stringify instead of
  raising.
- **Line 27** — the newline that makes it **ND**JSON (newline-delimited).
- **Line 28** — `yield hit` passes the *same* object through unchanged, so downstream sees exactly what
  was recorded. This is what makes it a "tee."
- **Lines 31–36** — `dump_to_ndjson`: the eager convenience wrapper. It drives `tee_to_ndjson` to
  exhaustion (line 34, the `for _ in ...` just pulls), counting hits, and returns the total. Useful
  when you want the file written *now* rather than as a side effect of someone else's iteration.

## 3. `src/es_extract/extract.py`

The two top-level entry points: `count` (the `_count` ground truth) and a one-call `iter_hits`.

```python
 1  """Top-level extraction helpers: ``count`` and a one-call ``iter_hits``.
 2
 3  ``iter_hits`` is the convenience entry point for callers who just want "stream
 4  every hit this query matches" without constructing a strategy by hand. It
 5  paginates with point-in-time + ``search_after`` (see :mod:`es_extract.pagination`).
 6  """
 7
 8  from __future__ import annotations
 9
10  from collections.abc import Iterator
11  from typing import Any
12
13  from es_extract.errors import EsExtractError
14  from es_extract.pagination import SearchAfterPagination
15
16
17  def count(
18      es: Any, index: str, query: dict[str, Any], *,
19      error_cls: type[Exception] = EsExtractError,
20  ) -> int:
21      """Return how many documents ``query`` matches in ``index`` (``_count``)."""
22      try:
23          resp = es.count(index=index, body={"query": query})
24      except Exception as e:
25          raise error_cls(f"count failed: {e!r}") from e
26      return int(resp.get("count", 0))
27
28
29  def iter_hits(
30      es: Any, index: str, query: dict[str, Any], *,
31      page_size: int = 1000, keep_alive: str = "5m", source_only: bool = True,
32      error_cls: type[Exception] = EsExtractError,
33  ) -> Iterator[dict[str, Any]]:
34      """Stream every hit ``query`` matches via point-in-time + ``search_after``.
35
36      ``keep_alive`` / ``source_only`` / ``error_cls`` configure the underlying
37      :class:`~es_extract.pagination.SearchAfterPagination`. Pass
38      ``source_only=False`` to receive the full hit envelope (``_id``, ``_score``,
39      ``sort``) instead of just ``_source``.
40      """
41      strategy = SearchAfterPagination(
42          keep_alive=keep_alive, source_only=source_only, error_cls=error_cls
43      )
44      return strategy.iter_hits(es=es, index=index, query=query, page_size=page_size)
```

**Line-by-line:**
- **Lines 1–6** — docstring: `iter_hits` is the "just stream everything" front door; it delegates to
  the PIT strategy.
- **Lines 10–14** — imports: `Iterator` for the return type; `Any` for `es`/values; the package's own
  `EsExtractError` (default wrap type) and the `SearchAfterPagination` strategy it builds.
- **Lines 17–20** — `count` signature. `es, index, query` are positional; everything after `*` is
  keyword-only. `error_cls` defaults to `EsExtractError` but a host can inject its own. Returns `int`.
- **Line 22–23** — issue the ES `_count`. `body={"query": query}` is the 8.x form (the project pins
  `elasticsearch>=8,<9` precisely because 9.x removes `body=`).
- **Line 24–25** — any failure is wrapped in `error_cls`. `from e` preserves the original traceback
  (exception chaining); `{e!r}` embeds the repr for diagnostics. Catching broad `Exception` here is
  deliberate: the package can't know the client's exception taxonomy, so it normalizes everything.
- **Line 26** — return the count, defaulting to `0` if the key is absent, coerced to `int`.
- **Lines 29–33** — `iter_hits` signature. Note the tunables with sensible defaults: `page_size=1000`,
  `keep_alive="5m"`, `source_only=True`, injectable `error_cls`. Returns an `Iterator` (lazy stream).
- **Lines 34–40** — docstring calls out the `source_only=False` escape hatch for callers that need the
  full hit envelope (e.g., the document `_id`).
- **Lines 41–43** — construct the strategy with the three tunables. This is the only place the strategy
  is built in the convenience path.
- **Line 44** — delegate to the strategy's `iter_hits` and return its generator. `page_size` is passed
  per-call (it's a property of the request, not the strategy object).

## 4. `src/es_extract/pagination.py`

The heart of the package: the point-in-time + `search_after` streaming generator.

```python
 1  """Point-in-time + ``search_after`` Elasticsearch pagination.
 2
 3  A single streaming strategy: :class:`SearchAfterPagination` opens a
 4  point-in-time (PIT), pages through every matching hit with ``search_after``,
 5  and **always** closes the PIT in a ``finally`` block — even if the consumer
 6  abandons iteration early. It is a generator, so it stays memory-bounded (one
 7  page at a time) regardless of how large the result set is.
 8
 9  It is duck-typed against the official ``elasticsearch`` client but accepts any
10  object exposing ``open_point_in_time`` / ``search`` / ``close_point_in_time``,
11  so it is trivially testable with a fake.
12
13  Two knobs distinguish it from an application-specific extractor:
14
15  * ``source_only`` — when ``True`` (default) yield each hit's ``_source``; when
16    ``False`` yield the full hit envelope (``_id``, ``_score``, ``sort``, …).
17  * ``error_cls`` — the exception type a request failure is wrapped in, so a host
18    application can map failures into its own hierarchy (see ``errors.py``).
19  """
20
21  from __future__ import annotations
22
23  import logging
24  from collections.abc import Iterator
25  from dataclasses import dataclass
26  from typing import Any
27
28  from es_extract.errors import EsExtractError
29
30  _log = logging.getLogger(__name__)
31
32
33  def _emit(hit: dict[str, Any], *, source_only: bool) -> dict[str, Any]:
34      """Return the hit's ``_source`` (default) or the whole envelope."""
35      if source_only:
36          source: Any = hit.get("_source", {})
37          return source if isinstance(source, dict) else {}
38      return hit
39
40
41  @dataclass
42  class SearchAfterPagination:
43      """Point-in-time + ``search_after`` pagination (Elastic's recommended
44      deep-pagination mechanism).
45
46      Each call to :meth:`iter_hits` owns one PIT for the lifetime of the
47      generator and releases it on completion *or* early abandonment.
48      """
49
50      keep_alive: str = "5m"
51      source_only: bool = True
52      error_cls: type[Exception] = EsExtractError
53
54      def iter_hits(
55          self, *, es: Any, index: str, query: dict[str, Any], page_size: int
56      ) -> Iterator[dict[str, Any]]:
57          try:
58              pit = es.open_point_in_time(index=index, keep_alive=self.keep_alive)
59          except Exception as e:
60              raise self.error_cls(f"open_point_in_time failed: {e!r}") from e
61          pit_id: str | None = pit.get("id")
62          try:
63              search_after: list[Any] | None = None
64              while True:
65                  body: dict[str, Any] = {
66                      "size": page_size,
67                      "query": query,
68                      "pit": {"id": pit_id, "keep_alive": self.keep_alive},
69                      "sort": [{"_shard_doc": "asc"}],
70                      "track_total_hits": False,
71                  }
72                  if search_after is not None:
73                      body["search_after"] = search_after
74                  try:
75                      resp = es.search(body=body)
76                  except Exception as e:
77                      raise self.error_cls(f"search_after page failed: {e!r}") from e
78                  pit_id = resp.get("pit_id", pit_id)
79                  hits = resp.get("hits", {}).get("hits", [])
80                  if not hits:
81                      break
82                  for h in hits:
83                      yield _emit(h, source_only=self.source_only)
84                  last_sort = hits[-1].get("sort")
85                  if not last_sort:
86                      break
87                  search_after = last_sort
88          finally:
89              if pit_id is not None:
90                  try:
91                      es.close_point_in_time(body={"id": pit_id})
92                  except Exception as e:  # best-effort: the PIT keep-alive reaps it anyway
93                      _log.warning("close_point_in_time failed: %r", e)
```

**Line-by-line:**
- **Lines 1–19** — docstring: one streaming strategy; PIT always closed in `finally`; duck-typed `es`;
  the two knobs (`source_only`, `error_cls`).
- **Lines 23–28** — imports: `logging` (for the best-effort cleanup warning), `Iterator`, `dataclass`,
  `Any`, and the default `EsExtractError`.
- **Line 30** — module logger named after the module (`es_extract.pagination`), the standard idiom.
- **Lines 33–38** — `_emit`, a tiny private helper deciding what each hit yields. Line 35–37: when
  `source_only`, pull `_source` (defaulting to `{}`); the `isinstance(source, dict)` guard protects
  against a malformed hit whose `_source` isn't a dict, returning `{}` instead of leaking a non-dict.
  Line 38: otherwise yield the whole hit envelope.
- **Line 41** — `@dataclass` generates `__init__`/`__repr__` from the fields below (no `frozen=True`
  here — the object is cheap and short-lived, immutability isn't needed).
- **Lines 50–52** — the three configurable fields with defaults: `keep_alive`, `source_only`,
  `error_cls`. These are *strategy*-level (set once); `page_size` is *per-call* (see line 55).
- **Lines 54–56** — `iter_hits` is keyword-only after `self` (`*`). It's a generator (has `yield`), so
  calling it returns immediately and runs lazily as the caller pulls.
- **Lines 57–60** — open the PIT. Note this is **outside** the main `try/finally`: if *opening* fails
  there's nothing to close, so we wrap-and-raise here and never enter the cleanup block.
- **Line 61** — capture the PIT id (typed `str | None` because `.get` may return `None`).
- **Line 62** — the `try` whose `finally` (line 88) guarantees the PIT is closed.
- **Line 63** — `search_after` starts `None` (first page has no cursor).
- **Line 64** — loop forever; we `break` out on an empty page or a missing cursor.
- **Lines 65–71** — build the search body each iteration: `size` (page size), the `query`, the `pit`
  reference with its `keep_alive` (renewing the PIT each request), `sort` on `_shard_doc` (the cheap,
  total, stable tie-breaker valid only inside a PIT), and `track_total_hits: False` (we don't pay for
  an exact total — the validator gets that separately via `_count`).
- **Lines 72–73** — on every page after the first, attach the cursor.
- **Lines 74–77** — issue the search; wrap any failure in `error_cls` with chaining.
- **Line 78** — refresh `pit_id` from the response (`pit_id` can be rotated by ES; fall back to the
  current one if absent).
- **Line 79** — defensively dig out `hits.hits`, defaulting to `[]` at each level.
- **Lines 80–81** — an empty page means we've drained the result set → stop.
- **Lines 82–83** — yield each hit through `_emit` (so `source_only` is honored). Streaming: one hit at
  a time, never the whole set.
- **Lines 84–87** — compute the next cursor from the **last** hit's `sort` array. If it's missing
  (`not last_sort`), stop (can't page further safely); otherwise feed it back as `search_after`.
- **Lines 88–93** — the `finally`: if we have a `pit_id`, close it. The inner `try/except` makes
  cleanup **best-effort** — a failed close is logged, not raised, so it can't mask the real exception
  that may have triggered the `finally`. The PIT's `keep_alive` would reap it anyway.

## 5. `src/es_extract/__init__.py`

The public surface of the package.

```python
 1  """Standalone, dependency-light Elasticsearch extraction.
 2
 3  Depends only on the standard library and a duck-typed Elasticsearch client
 4  (any object exposing ``count`` / ``search`` / ``open_point_in_time`` /
 5  ``close_point_in_time``). It has **no** dependency on the rest of this
 6  repository, so the package can be copied or installed and reused on its own.
 7
 8  Extraction uses point-in-time + ``search_after`` — Elastic's recommended
 9  deep-pagination mechanism — exposed as a memory-bounded streaming generator.
10
11  Quick start::
12
13      from elasticsearch import Elasticsearch
14      from es_extract import count, iter_hits
15
16      es = Elasticsearch("http://localhost:9200")
17      q = {"match_all": {}}
18      print(count(es, "my-index", q))
19      for src in iter_hits(es, "my-index", q):
20          ...  # `src` is each hit's _source dict (pass source_only=False for the envelope)
21  """
22
23  from __future__ import annotations
24
25  from es_extract.diagnostics import dump_to_ndjson, tee_to_ndjson
26  from es_extract.errors import EsExtractError
27  from es_extract.extract import count, iter_hits
28  from es_extract.pagination import SearchAfterPagination
29
30  __all__ = [
31      "EsExtractError",
32      "SearchAfterPagination",
33      "count",
34      "dump_to_ndjson",
35      "iter_hits",
36      "tee_to_ndjson",
37  ]
38
39  __version__ = "0.2.0"
```

**Line-by-line:**
- **Lines 1–21** — package docstring with a runnable quick-start; it re-states the zero-dependency
  promise and the PIT mechanism.
- **Lines 25–28** — re-export the package's public names from their modules so callers write
  `from es_extract import count, iter_hits` rather than reaching into submodules. Imports are sorted
  (ruff's `I` rule).
- **Lines 30–37** — `__all__` defines the public API explicitly: it controls `from es_extract import *`
  and signals intent to readers and tools. Note it's its **own** version (`0.2.0`, line 39) — distinct
  from `etl`'s `0.1.0` — reinforcing that it versions independently as a reusable library.

---

# Part II — `etl` (the application)

## 6. `src/etl/__init__.py`

```python
1  __version__ = "0.1.0"
```

**Line-by-line:**
- **Line 1** — the only line: the application's version string. Its presence also makes `src/etl/` a
  regular package. (Distinct from `es_extract`'s `0.2.0` — they version separately.)

## 7. `src/etl/errors.py`

The exception hierarchy. One base, `EtlError`, lets the daemon catch every recoverable failure at a
single boundary.

```python
 1  """Exception hierarchy for the ETL pipeline.
 2
 3  All recoverable, job-scoped failures inherit from `EtlError` so the consumer
 4  loop can catch them at the boundary and keep running. Anything not derived
 5  from `EtlError` (e.g., import errors, programmer bugs) is allowed to crash
 6  the process.
 7  """
 8
 9  from __future__ import annotations
10
11
12  class EtlError(Exception):
13      """Base class for all ETL-pipeline errors."""
14
15
16  class ConfigError(EtlError):
17      """Raised when required configuration is missing or invalid."""
18
19
20  class ControlMessageError(EtlError):
21      """Raised when a Kafka control message can't be decoded or is missing fields."""
22
23
24  class JobSpecError(EtlError):
25      """Raised when the ES-resident job document is missing or malformed."""
26
27
28  class ElasticsearchQueryError(EtlError):
29      """Raised on ES request failures (count, search, PIT)."""
30
31
32  class TransformError(EtlError):
33      """Raised when applying the JOLT spec to an ES hit fails.
34
35      Carries the job_id, hit_id (if any), and the offending JOLT operation
36      name so the failure can be diagnosed without re-reading the full doc.
37      """
38
39      def __init__(self, message: str, *, job_id: str, hit_id: str | None = None,
40                   jolt_op: str | None = None) -> None:
41          super().__init__(message)
42          self.job_id = job_id
43          self.hit_id = hit_id
44          self.jolt_op = jolt_op
45
46
47  class CsvWriteError(EtlError):
48      """Raised on local CSV / sidecar write failures."""
49
50
51  class RecordCountMismatch(EtlError):
52      """Raised when ES `_count` and CSV row count disagree after all retries."""
53
54      def __init__(self, expected: int, actual: int, attempts: list[tuple[int, int]]) -> None:
55          super().__init__(
56              f"record count mismatch: expected={expected} actual={actual} "
57              f"attempts={attempts}"
58          )
59          self.expected = expected
60          self.actual = actual
61          self.attempts = attempts
62
63
64  class SftpUploadError(EtlError):
65      """Raised when the sftp subprocess exits non-zero or times out."""
66
67
68  class RetryExhausted(EtlError):
69      """Wraps the final exception after retry attempts are exhausted.
70
71      Only raised when the retry decorator is invoked with wrap_final=True.
72      The default behaviour is to re-raise the original exception unchanged.
73      """
74
75      def __init__(self, attempts: int, last_exc: BaseException) -> None:
76          super().__init__(f"retry exhausted after {attempts} attempts: {last_exc!r}")
77          self.attempts = attempts
78          self.last_exc = last_exc
```

**Line-by-line:**
- **Lines 1–7** — docstring states the contract: `EtlError` = recoverable + job-scoped (caught at the
  loop boundary); anything else crashes the process (a bug should be loud).
- **Line 12–13** — `EtlError` the base; empty body, it's a pure marker type.
- **Lines 16–17** — `ConfigError`: startup misconfiguration. (Handled specially — `__main__` catches it
  *before* the loop and exits 2.)
- **Lines 20–21** — `ControlMessageError`: an undecodable/malformed Kafka message.
- **Lines 24–25** — `JobSpecError`: the ES job document is missing or malformed.
- **Lines 28–29** — `ElasticsearchQueryError`: any ES request failure (this is the `error_cls` the
  extractor injects into `es_extract`).
- **Lines 32–44** — `TransformError`, the first class with a custom `__init__`. It carries `job_id` and
  optional `hit_id` for diagnosis. **Known staleness (REVIEW §3.2):** the docstring still says "JOLT"
  and the `jolt_op` field (line 40, 44) is **dead** — JOLT was dropped for dotted-path projection and
  nothing sets/reads `jolt_op`. Kept aligned with the tutorial snippet; flagged as deferred cleanup.
  Line 41 `super().__init__(message)` sets the message; 42–44 attach the structured fields.
- **Lines 47–48** — `CsvWriteError`: local CSV/sidecar write failure.
- **Lines 51–61** — `RecordCountMismatch`. Its `__init__` builds a human message (55–58) *and* stores
  structured fields `expected`/`actual`/`attempts` (59–61) — `attempts` is the full list of
  `(es_count, csv_rows)` pairs the validator tried, so a failure is fully auditable.
- **Lines 64–65** — `SftpUploadError`: the sftp subprocess failed/timed out.
- **Lines 68–78** — `RetryExhausted`: wraps the final exception **only** when `wrap_final=True`
  (default retry behavior re-raises the original). Stores `attempts` and `last_exc` (77–78) for
  inspection.

## 8. `src/etl/models.py`

The plain, immutable data types passed between stages.

```python
 1  """Plain-data types passed between pipeline stages."""
 2
 3  from __future__ import annotations
 4
 5  from dataclasses import dataclass
 6  from pathlib import Path
 7  from typing import Any
 8
 9
10  @dataclass(frozen=True)
11  class ControlMessage:
12      """A decoded message from the Kafka control topic.
13
14      `raw_partition` and `raw_offset` are kept so the orchestrator can commit
15      the exact offset back to Kafka after a successful job.
16      """
17
18      job_doc_id: str
19      correlation_id: str | None
20      raw_partition: int
21      raw_offset: int
22
23
24  @dataclass(frozen=True)
25  class JobSpec:
26      """Describes a single export job, loaded by id from the ES job-index.
27
28      `query` is the ES query body (DSL). `column_paths` maps each CSV column
29      name to a dotted path into the source document (e.g. ``user.id``);
30      columns absent from the mapping fall back to a same-name top-level
31      lookup. `columns` controls CSV column order.
32      """
33
34      job_id: str
35      data_index: str
36      query: dict[str, Any]
37      column_paths: dict[str, str]
38      columns: list[str]
39      remote_filename: str
40
41
42  @dataclass(frozen=True)
43  class CsvResult:
44      """Result of streaming an iterator of rows to disk."""
45
46      csv_path: Path
47      sidecar_path: Path
48      row_count: int
49      sha256_hex: str
```

**Line-by-line:**
- **Line 1** — one-line module docstring: these are inter-stage value types.
- **Lines 5–7** — imports: `dataclass`, `Path` (filesystem types), `Any` (the ES query/values).
- **Line 10** — `@dataclass(frozen=True)`: immutable. Frozen instances can't be mutated after
  construction, so values can't drift as they pass between stages (and they're hashable).
- **Lines 11–21** — `ControlMessage`: the decoded Kafka message. Fields: `job_doc_id` (the pointer),
  optional `correlation_id`, and `raw_partition`/`raw_offset` — kept (per the docstring) so the
  orchestrator can commit *that exact* offset after success.
- **Lines 24–39** — `JobSpec`: the validated job definition. `job_id`, `data_index`, `query` (DSL),
  `column_paths` (column→dotted-path map), `columns` (ordered output columns), `remote_filename`. The
  docstring explains the fallback rule (a column with no path maps to its own top-level name).
- **Lines 42–49** — `CsvResult`: what `write_csv` returns — both paths (csv + sidecar), the
  `row_count`, and the `sha256_hex`. Enough for the validator and uploader to act without re-reading
  the file.

## 9. `src/etl/logging_setup.py`

Structured JSON logging: one object per line, with `extra` fields merged in.

```python
 1  """Minimal structured-JSON logging.
 2
 3  Each record is one JSON object per line. The `extra` dict on `logger.info(...)`
 4  is merged into the payload so callers can attach `job_id`, `correlation_id`,
 5  attempt numbers, etc. without juggling format strings.
 6  """
 7
 8  from __future__ import annotations
 9
10  import json
11  import logging
12  import sys
13  from typing import Any
14
15  _RESERVED = {
16      "args", "asctime", "created", "exc_info", "exc_text", "filename",
17      "funcName", "levelname", "levelno", "lineno", "message", "module",
18      "msecs", "msg", "name", "pathname", "process", "processName",
19      "relativeCreated", "stack_info", "thread", "threadName", "taskName",
20  }
21
22
23  class JsonFormatter(logging.Formatter):
24      def format(self, record: logging.LogRecord) -> str:
25          payload: dict[str, Any] = {
26              "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
27              "level": record.levelname,
28              "logger": record.name,
29              "msg": record.getMessage(),
30          }
31          for key, value in record.__dict__.items():
32              if key not in _RESERVED and not key.startswith("_"):
33                  payload[key] = value
34          if record.exc_info:
35              payload["exc"] = self.formatException(record.exc_info)
36          return json.dumps(payload, default=str)
37
38
39  def configure_logging(level: str = "INFO") -> None:
40      root = logging.getLogger()
41      for h in list(root.handlers):
42          root.removeHandler(h)
43      handler = logging.StreamHandler(stream=sys.stdout)
44      handler.setFormatter(JsonFormatter())
45      root.addHandler(handler)
46      root.setLevel(level)
```

**Line-by-line:**
- **Lines 1–6** — docstring: one JSON object per line; `extra=` fields are merged into the payload.
- **Lines 10–13** — imports: `json` (serialize), `logging` (base classes), `sys` (stdout), `Any`.
- **Lines 15–20** — `_RESERVED`: the set of standard `LogRecord` attribute names. We exclude these when
  copying user-supplied `extra` so the payload isn't polluted with the dozens of built-in record
  fields. Defined once at module scope (a `set` for O(1) membership).
- **Line 23** — `JsonFormatter` subclasses `logging.Formatter`; overriding `format` controls the
  rendered line.
- **Lines 25–30** — start the payload with the four always-present keys: ISO-ish timestamp (`formatTime`
  with a `%z` offset), level name, logger name, and the rendered message (`getMessage()` applies any
  `%`-args).
- **Lines 31–33** — the merge: iterate the record's `__dict__`, copying any key that's *not* reserved
  and doesn't start with `_`. This is exactly how `extra={"job_id": ...}` ends up in the JSON.
- **Lines 34–35** — if the record carries exception info, render the traceback into an `exc` field.
- **Line 36** — serialize. `default=str` ensures non-JSON values (paths, datetimes) stringify rather
  than raise.
- **Lines 39–46** — `configure_logging`: get the root logger (40); remove any pre-existing handlers
  (41–42) so repeated calls (and pytest) don't double-log; attach a single stdout `StreamHandler` with
  our formatter (43–45); set the level (46). Logging to **stdout** (not stderr) suits container log
  collection.

## 10. `src/etl/config.py`

Environment-driven, fail-fast, immutable configuration.

```python
  1  """Environment-driven configuration.
  2
  3  All settings are loaded once at process start. Missing required values raise
  4  `ConfigError` immediately — the daemon refuses to start with a bad config.
  5  """
  6
  7  from __future__ import annotations
  8
  9  import os
 10  from dataclasses import dataclass
 11  from pathlib import Path
 12
 13  from dotenv import load_dotenv
 14
 15  from etl.errors import ConfigError
 16
 17
 18  def _get(key: str, default: str | None = None, *, required: bool = False) -> str:
 19      val = os.environ.get(key, default)
 20      if required and (val is None or val == ""):
 21          raise ConfigError(f"missing required env var: {key}")
 22      return val if val is not None else ""
 23
 24
 25  def _get_int(key: str, default: int) -> int:
 26      raw = os.environ.get(key)
 27      if raw is None or raw == "":
 28          return default
 29      try:
 30          return int(raw)
 31      except ValueError as e:
 32          raise ConfigError(f"{key} must be an integer, got: {raw!r}") from e
 33
 34
 35  def _get_float(key: str, default: float) -> float:
 36      raw = os.environ.get(key)
 37      if raw is None or raw == "":
 38          return default
 39      try:
 40          return float(raw)
 41      except ValueError as e:
 42          raise ConfigError(f"{key} must be a float, got: {raw!r}") from e
```

**Line-by-line (helpers):**
- **Lines 1–5** — docstring: load once at startup; missing required → `ConfigError` (fail fast).
- **Lines 9–15** — imports: `os` (env), `dataclass`, `Path`, `load_dotenv` (the `.env` reader), and the
  project's `ConfigError`.
- **Lines 18–22** — `_get`: read a string env var. If `required` and missing/empty → raise (20–21).
  Otherwise return the value, coalescing `None`→`""` (22) so the return type is always `str`.
- **Lines 25–32** — `_get_int`: read, treat missing/empty as the default (27–28), else parse `int`
  (30) and wrap a parse failure in `ConfigError` (31–32). **Note (adversarial finding):** this checks
  *parseability* only — `PAGE_SIZE=0` or `-5` parse fine and are accepted (no range check).
- **Lines 35–42** — `_get_float`: the float analogue, same shape.

```python
 45  @dataclass(frozen=True)
 46  class KafkaConfig:
 47      bootstrap_servers: str
 48      control_topic: str
 49      group_id: str
 50      security_protocol: str | None = None
 51      sasl_mechanism: str | None = None
 52      sasl_username: str | None = None
 53      sasl_password: str | None = None
 54
 55      def confluent_config(self) -> dict[str, str]:
 56          cfg: dict[str, str] = {
 57              "bootstrap.servers": self.bootstrap_servers,
 58              "group.id": self.group_id,
 59              "enable.auto.commit": "false",
 60              "auto.offset.reset": "earliest",
 61          }
 62          if self.security_protocol:
 63              cfg["security.protocol"] = self.security_protocol
 64          if self.sasl_mechanism:
 65              cfg["sasl.mechanism"] = self.sasl_mechanism
 66          if self.sasl_username:
 67              cfg["sasl.username"] = self.sasl_username
 68          if self.sasl_password:
 69              cfg["sasl.password"] = self.sasl_password
 70          return cfg
```

**Line-by-line (`KafkaConfig`):**
- **Lines 45–53** — frozen dataclass: three required fields, then four optional SASL/SSL fields
  defaulting to `None` (only needed for secured clusters).
- **Lines 55–61** — `confluent_config` maps our fields to the `confluent_kafka` config dict. The two
  hard-coded values matter: `enable.auto.commit=false` (we commit manually after success) and
  `auto.offset.reset=earliest` (a brand-new group starts at the beginning, so no messages are missed).
- **Lines 62–69** — conditionally add each SASL/SSL key only if set (truthy), so an unsecured local
  cluster gets a clean minimal config.
- **Line 70** — return the assembled dict.

```python
 73  @dataclass(frozen=True)
 74  class EsConfig:
 75      hosts: list[str]
 76      username: str | None
 77      password: str | None
 78      api_key: str | None
 79      job_index: str
 80
 81
 82  @dataclass(frozen=True)
 83  class PaginationConfig:
 84      page_size: int
 85      pit_keep_alive: str
 86
 87
 88  @dataclass(frozen=True)
 89  class RetryConfig:
 90      max_attempts: int = 5
 91      backoff_base: float = 1.0
 92      backoff_cap: float = 30.0
 93      jitter: float = 0.25
 94
 95
 96  @dataclass(frozen=True)
 97  class SftpConfig:
 98      host: str
 99      port: int
100      user: str
101      key_path: Path
102      remote_dir: str
103      known_hosts: Path
104
105
106  @dataclass(frozen=True)
107  class Settings:
108      kafka: KafkaConfig
109      es: EsConfig
110      pagination: PaginationConfig
111      retry: RetryConfig
112      sftp: SftpConfig
113      csv_output_dir: Path
114      log_level: str = "INFO"
115      # Optional diagnostic: when set, each job tees its raw extracted hits to
116      # `<raw_dump_dir>/<job_id>.ndjson`. Unset (default) disables the dump.
117      raw_dump_dir: Path | None = None
```

**Line-by-line (the config dataclasses):**
- **Lines 73–79** — `EsConfig`: `hosts` is a *list* (comma-split at load), the auth trio is optional,
  `job_index` is where job docs live.
- **Lines 82–85** — `PaginationConfig`: now just `page_size` + `pit_keep_alive` — **PIT-only** after the
  Scroll removal (no more `strategy`/`scroll_keep_alive`).
- **Lines 88–93** — `RetryConfig`: all fields have defaults, so a bare `RetryConfig()` is valid. These
  drive both the SFTP retry and the validator's count-retry.
- **Lines 96–103** — `SftpConfig`: connection + auth (`key_path`), plus `remote_dir` (a reserved base
  dir) and `known_hosts` (the strict host-key file). Paths are `Path`, not `str`.
- **Lines 106–117** — `Settings`: the aggregate root holding all sub-configs plus `csv_output_dir`,
  `log_level` (default INFO), and the optional `raw_dump_dir` diagnostic (None = disabled).

```python
120  def load_settings(*, dotenv_path: str | None = None) -> Settings:
121      """Load and validate settings from environment (and optional .env file)."""
122      load_dotenv(dotenv_path=dotenv_path, override=False)
123
124      kafka = KafkaConfig(
125          bootstrap_servers=_get("KAFKA_BOOTSTRAP_SERVERS", required=True),
126          control_topic=_get("KAFKA_CONTROL_TOPIC", required=True),
127          group_id=_get("KAFKA_GROUP_ID", required=True),
128          security_protocol=_get("KAFKA_SECURITY_PROTOCOL") or None,
129          sasl_mechanism=_get("KAFKA_SASL_MECHANISM") or None,
130          sasl_username=_get("KAFKA_SASL_USERNAME") or None,
131          sasl_password=_get("KAFKA_SASL_PASSWORD") or None,
132      )
133
134      hosts_raw = _get("ES_HOSTS", required=True)
135      es = EsConfig(
136          hosts=[h.strip() for h in hosts_raw.split(",") if h.strip()],
137          username=_get("ES_USERNAME") or None,
138          password=_get("ES_PASSWORD") or None,
139          api_key=_get("ES_API_KEY") or None,
140          job_index=_get("ES_JOB_INDEX", required=True),
141      )
142
143      pagination = PaginationConfig(
144          page_size=_get_int("PAGE_SIZE", 1000),
145          pit_keep_alive=_get("PIT_KEEP_ALIVE", "5m"),
146      )
147
148      retry = RetryConfig(
149          max_attempts=_get_int("RETRY_MAX_ATTEMPTS", 5),
150          backoff_base=_get_float("RETRY_BACKOFF_BASE", 1.0),
151          backoff_cap=_get_float("RETRY_BACKOFF_CAP", 30.0),
152          jitter=_get_float("RETRY_JITTER", 0.25),
153      )
154
155      sftp = SftpConfig(
156          host=_get("SFTP_HOST", required=True),
157          port=_get_int("SFTP_PORT", 22),
158          user=_get("SFTP_USER", required=True),
159          key_path=Path(_get("SFTP_KEY_PATH", required=True)),
160          # Optional: the job document's `remote_filename` is the authoritative
161          # remote path. `remote_dir` is reserved for callers that want to build
162          # remote paths from a base dir; it is not required to start the daemon.
163          remote_dir=_get("SFTP_REMOTE_DIR"),
164          known_hosts=Path(_get("SFTP_KNOWN_HOSTS", required=True)),
165      )
166
167      return Settings(
168          kafka=kafka,
169          es=es,
170          pagination=pagination,
171          retry=retry,
172          sftp=sftp,
173          csv_output_dir=Path(_get("CSV_OUTPUT_DIR", "/tmp/etl-csv")),
174          log_level=_get("LOG_LEVEL", "INFO").upper(),
175          raw_dump_dir=Path(raw_dump) if (raw_dump := _get("ES_RAW_DUMP_DIR")) else None,
176      )
```

**Line-by-line (`load_settings`):**
- **Lines 120–121** — the single entry point; `dotenv_path` is injectable (tests pass one).
- **Line 122** — `load_dotenv(override=False)`: load a `.env` if present, but **real environment
  variables win** (override=False) — so container env can't be clobbered by a stray file.
- **Lines 124–132** — build `KafkaConfig`: three `required=True` reads (raise if missing); the optional
  SASL fields use `_get(...) or None` so empty string becomes `None`.
- **Lines 134–141** — `ES_HOSTS` is read then split on commas with whitespace stripped and empties
  dropped (136) → a clean `list[str]`. Auth fields optional; `job_index` required.
- **Lines 143–146** — pagination: `PAGE_SIZE` (int, default 1000) and `PIT_KEEP_ALIVE` (default "5m").
- **Lines 148–153** — retry knobs with their defaults via the typed helpers.
- **Lines 155–165** — SFTP: required host/user/key/known_hosts; `port` int (default 22); `remote_dir`
  optional (the comment explains the job's `remote_filename` is authoritative). `key_path`/`known_hosts`
  wrapped in `Path`.
- **Lines 167–176** — assemble `Settings`. Line 173 wraps `CSV_OUTPUT_DIR` (default `/tmp/etl-csv`) in a
  `Path`; line 174 upper-cases the log level so `info`→`INFO`; line 175 uses the **walrus** operator
  (`:=`) to read `ES_RAW_DUMP_DIR` once and wrap it in a `Path` only if non-empty, else `None`.

## 11. `src/etl/transformer.py`

Flattens each ES document into a CSV row using a small dotted-path language.

```python
 1  """Per-row JSON-to-flat-row projection.
 2
 3  The original design called for a JOLT transformation pass; we replaced JOLT
 4  with a simpler **dotted-path projection** because no maintained Python port
 5  of JOLT exists. Each CSV column is associated with a dotted path into the
 6  source document (e.g. column ``user_id`` maps to ``user.id``). Missing
 7  paths yield an empty string; columns absent from the mapping fall back to
 8  a same-name top-level lookup.
 9
10  Supported path syntax:
11
12  * ``a``           — top-level key
13  * ``a.b.c``       — nested object keys
14  * ``a.b[0]``      — list index (``[N]`` only, no slices)
15  * ``a.b[0].c``    — mixed
16
17  That covers every NiFi-style export we have today. If a job ever needs
18  something richer (wildcards, conditionals, transformations), add a custom
19  pre-step in this module rather than re-introducing a JOLT dependency.
20  """
21
22  from __future__ import annotations
23
24  import re
25  from collections.abc import Iterable, Iterator
26  from typing import Any
27
28  from etl.errors import TransformError
29
30  # Splits "users[0].name" → ["users", "[0]", "name"]
31  _TOKEN_RE = re.compile(r"\[\d+\]|[^.\[]+")
32  _INDEX_RE = re.compile(r"\[(\d+)\]")
33
34
35  def _tokens(path: str) -> list[str]:
36      return _TOKEN_RE.findall(path)
37
38
39  def get_by_path(doc: Any, path: str) -> Any:
40      """Resolve a dotted path inside `doc`. Returns "" when any step is missing."""
41      cur: Any = doc
42      for tok in _tokens(path):
43          idx_match = _INDEX_RE.fullmatch(tok)
44          if idx_match is not None:
45              idx = int(idx_match.group(1))
46              if isinstance(cur, list) and 0 <= idx < len(cur):
47                  cur = cur[idx]
48              else:
49                  return ""
50          else:
51              if isinstance(cur, dict) and tok in cur:
52                  cur = cur[tok]
53              else:
54                  return ""
55          if cur is None:
56              return ""
57      return cur
58
59
60  def project(
61      hit: dict[str, Any],
62      columns: list[str],
63      column_paths: dict[str, str] | None,
64      *,
65      job_id: str,
66      hit_id: str | None = None,
67  ) -> dict[str, Any]:
68      """Project one source dict into a flat {column: value} dict.
69
70      `column_paths` overrides the default name lookup for individual columns.
71      Raises `TransformError` if a configured path is syntactically empty.
72      """
73      paths = column_paths or {}
74      out: dict[str, Any] = {}
75      for col in columns:
76          path = paths.get(col, col)
77          if not path:
78              raise TransformError(
79                  f"empty path for column {col!r}", job_id=job_id, hit_id=hit_id,
80              )
81          out[col] = get_by_path(hit, path)
82      return out
83
84
85  def iter_transformed(
86      hits: Iterable[dict[str, Any]],
87      column_paths: dict[str, str] | None,
88      columns: list[str],
89      *,
90      job_id: str,
91  ) -> Iterator[dict[str, Any]]:
92      for hit in hits:
93          hit_id = hit.get("_id") if isinstance(hit, dict) else None
94          yield project(hit, columns, column_paths, job_id=job_id, hit_id=hit_id)
```

**Line-by-line:**
- **Lines 1–20** — docstring: why JOLT was dropped, the supported path syntax, and the guidance to add
  a Python pre-step rather than reintroduce JOLT.
- **Lines 24–28** — imports: `re` for the path tokenizer, the stream types, `Any`, and `TransformError`.
- **Lines 30–32** — two compiled regexes. `_TOKEN_RE` matches either a `[N]` index **or** a run of
  characters that aren't `.` or `[` (a key) — so `users[0].name` → `["users", "[0]", "name"]`.
  `_INDEX_RE` captures the digits inside `[N]`.
- **Lines 35–36** — `_tokens` just runs `findall`. **Edge case (adversarial #7):** a path of `"."`
  produces `[]` tokens, and `get_by_path` then returns `cur` unchanged (the whole document) — a known
  leak to fix.
- **Lines 39–41** — `get_by_path`: start `cur` at the whole doc and walk each token.
- **Lines 43–49** — index step: if the token is `[N]`, parse the int (45); if `cur` is a list and the
  index is in range, descend (46–47); otherwise return `""` (out-of-range/non-list → empty cell, not an
  error).
- **Lines 50–54** — key step: if `cur` is a dict containing the key, descend (51–52); else `""`.
- **Lines 55–56** — if any step lands on `None`, stop and return `""` (a present-but-null value is an
  empty cell).
- **Line 57** — return whatever we walked to (a scalar for normal paths; could be a dict/list for an
  object-valued path — which `_stringify` will later `repr`, adversarial #3).
- **Lines 60–67** — `project`: turn one hit into a `{column: value}` row. `hit_id` is accepted for
  `TransformError` context.
- **Line 73** — `paths = column_paths or {}` tolerates a `None` mapping.
- **Lines 75–81** — for each column: resolve its path (default = the column's own name, line 76); if the
  configured path is empty raise `TransformError` (77–80) — *operator* error, fail loud; otherwise
  resolve the value (81). Missing *data* is tolerated (empty cell) but a mis-written job is not.
- **Line 82** — return the assembled row.
- **Lines 85–94** — `iter_transformed`: the streaming generator over hits. Line 93 pulls `_id` for
  diagnostics — but note (adversarial #8) post-PIT each hit is already its `_source`, so `_id` is
  typically `None` here. Line 94 yields each projected row lazily.

## 12. `src/etl/csv_writer.py`

Streams rows to CSV while hashing the exact bytes in one pass, then writes a `sha256sum`-format sidecar.

```python
 1  """Streaming CSV writer that computes a SHA256 sidecar in the same pass.
 2
 3  The hash is updated incrementally as bytes are written to disk, so the file
 4  is hashed exactly once regardless of size.
 5  """
 6
 7  from __future__ import annotations
 8
 9  import csv
10  import hashlib
11  from collections.abc import Iterable
12  from pathlib import Path
13  from typing import Any, BinaryIO
14
15  from etl.errors import CsvWriteError
16  from etl.models import CsvResult
17
18
19  class _HashingWriter:
20      """File wrapper that mirrors writes into a hashlib hasher."""
21
22      def __init__(self, fh: BinaryIO, hasher: Any) -> None:
23          self._fh = fh
24          self._hasher = hasher
25
26      def write(self, s: str) -> int:
27          b = s.encode("utf-8")
28          self._fh.write(b)
29          self._hasher.update(b)
30          return len(s)
31
32
33  def write_csv(
34      rows: Iterable[dict[str, Any]],
35      columns: list[str],
36      csv_path: Path,
37  ) -> CsvResult:
38      if not columns:
39          raise CsvWriteError("columns must not be empty")
40      csv_path.parent.mkdir(parents=True, exist_ok=True)
41      hasher = hashlib.sha256()
42      row_count = 0
43      try:
44          with csv_path.open("wb") as fh:
45              wrapper = _HashingWriter(fh, hasher)
46              writer = csv.DictWriter(
47                  wrapper,
48                  fieldnames=columns,
49                  extrasaction="ignore",
50                  lineterminator="\n",
51              )
52              writer.writeheader()
53              for row in rows:
54                  writer.writerow({c: _stringify(row.get(c, "")) for c in columns})
55                  row_count += 1
56      except OSError as e:
57          raise CsvWriteError(f"failed writing csv {csv_path}: {e!r}") from e
58
59      sidecar_path = csv_path.with_suffix(csv_path.suffix + ".sha256")
60      digest = hasher.hexdigest()
61      try:
62          # sha256sum format: "<hex>  <filename>\n" (two spaces, basename only).
63          sidecar_path.write_text(f"{digest}  {csv_path.name}\n", encoding="utf-8")
64      except OSError as e:
65          raise CsvWriteError(f"failed writing sidecar {sidecar_path}: {e!r}") from e
66
67      return CsvResult(
68          csv_path=csv_path,
69          sidecar_path=sidecar_path,
70          row_count=row_count,
71          sha256_hex=digest,
72      )
73
74
75  def _stringify(v: Any) -> str:
76      if v is None:
77          return ""
78      if isinstance(v, bool):
79          return "true" if v else "false"
80      return str(v)
```

**Line-by-line:**
- **Lines 1–5** — docstring: the hash is computed incrementally during the write, so no second read
  pass.
- **Lines 9–16** — imports: `csv`, `hashlib`, `Iterable`, `Path`, `Any`/`BinaryIO`, plus `CsvWriteError`
  and the `CsvResult` model.
- **Lines 19–24** — `_HashingWriter`: a decorator over a binary file. It stores the real file handle and
  a hasher.
- **Lines 26–30** — `write` is the trick: encode the incoming `str` to UTF-8 bytes (27), write those
  bytes to disk (28) **and** feed the same bytes to the hasher (29) — so the on-disk file and the digest
  are guaranteed to match. **Known nit (adversarial #1):** line 30 returns `len(s)` (characters) rather
  than `len(b)` (bytes written); `csv.DictWriter` ignores the return, so it's harmless today but
  violates the `io` contract.
- **Lines 33–37** — `write_csv` signature: a lazy `Iterable` of row dicts, the ordered `columns`, the
  target path. Returns a `CsvResult`.
- **Lines 38–39** — guard: empty `columns` is a misconfiguration → `CsvWriteError`.
- **Line 40** — ensure the output directory exists.
- **Lines 41–42** — create the sha256 hasher and a row counter.
- **Lines 43–44** — open the file **binary** (`"wb"`) inside a `with` (auto-close even on error).
- **Lines 45–51** — wrap the file in `_HashingWriter` and hand it to `csv.DictWriter`. Three choices
  matter: `extrasaction="ignore"` (drop extra dict keys rather than raise), `lineterminator="\n"`
  (stable bytes across platforms — no `\r\n`), and writing through the wrapper so everything is hashed.
- **Line 52** — write the header row (the column names).
- **Lines 53–55** — stream each row: project to the exact column set, `_stringify` each value, write,
  and count. `row.get(c, "")` fills missing columns with empty cells.
- **Lines 56–57** — any `OSError` during the write becomes a `CsvWriteError` (chained).
- **Lines 59–60** — derive the sidecar path (`<file>.sha256`) and finalize the digest.
- **Lines 61–65** — write the sidecar in `sha256sum -c` format: `"<hex>  <basename>\n"` (two spaces,
  basename only so verification works in the delivery dir). Wrap failures.
- **Lines 67–72** — return the `CsvResult` with both paths, the row count, and the hex digest.
- **Lines 75–80** — `_stringify`: `None`→`""` (76–77), `bool`→lowercase `"true"/"false"` (78–79; note
  this must precede the general case because `bool` is an `int` subclass), everything else `str(v)`
  (80). **Edge (adversarial #3):** a dict/list value becomes a Python `repr`, not JSON.

## 13. `src/etl/retry.py`

Generic exponential-backoff-with-jitter retry, with an injectable clock for instant tests.

```python
  1  """Generic exponential-backoff-with-jitter retry helper.
  2
  3  `retry_call` is the building block. `@retry(...)` is the decorator form.
  4  The `sleeper` argument is exposed so tests can pass a fake clock instead of
  5  calling `time.sleep` for real.
  6  """
  7
  8  from __future__ import annotations
  9
 10  import logging
 11  import random
 12  import time
 13  from collections.abc import Callable
 14  from functools import wraps
 15  from typing import Any, TypeVar
 16
 17  from etl.errors import RetryExhausted
 18
 19  T = TypeVar("T")
 20
 21  _log = logging.getLogger(__name__)
 22
 23
 24  def _compute_delay(attempt: int, base: float, cap: float, jitter: float,
 25                     rng: random.Random) -> float:
 26      bounded: float = min(cap, base * (2 ** attempt))
 27      if jitter > 0:
 28          bounded *= 1.0 + rng.uniform(-jitter, jitter)
 29      return max(0.0, bounded)
 30
 31
 32  def retry_call(
 33      fn: Callable[..., T],
 34      *args: Any,
 35      on: tuple[type[BaseException], ...],
 36      attempts: int = 5,
 37      base: float = 1.0,
 38      cap: float = 30.0,
 39      jitter: float = 0.25,
 40      sleeper: Callable[[float], None] = time.sleep,
 41      rng: random.Random | None = None,
 42      wrap_final: bool = False,
 43      log_extra: dict[str, Any] | None = None,
 44      **kwargs: Any,
 45  ) -> T:
 46      """Call `fn` with retry-on-exception semantics.
 47
 48      Re-raises the original exception after the final attempt unless
 49      `wrap_final=True`, in which case it raises `RetryExhausted` wrapping it.
 50      """
 51      if attempts < 1:
 52          raise ValueError("attempts must be >= 1")
 53      rng = rng or random.Random()
 54      last: BaseException | None = None
 55      for attempt in range(attempts):
 56          try:
 57              return fn(*args, **kwargs)
 58          except on as exc:
 59              last = exc
 60              if attempt == attempts - 1:
 61                  break
 62              delay = _compute_delay(attempt, base, cap, jitter, rng)
 63              _log.warning(
 64                  "retry: %s attempt=%d/%d delay=%.3fs err=%r",
 65                  getattr(fn, "__name__", repr(fn)),
 66                  attempt + 1,
 67                  attempts,
 68                  delay,
 69                  exc,
 70                  extra={**(log_extra or {}), "retry_attempt": attempt + 1,
 71                         "retry_delay_s": delay},
 72              )
 73              sleeper(delay)
 74      assert last is not None
 75      if wrap_final:
 76          raise RetryExhausted(attempts, last) from last
 77      raise last
```

**Line-by-line:**
- **Lines 1–6** — docstring: `retry_call` is the core, `@retry` the decorator, `sleeper` injectable.
- **Lines 10–17** — imports: `logging`, `random` (jitter), `time` (default sleeper), `Callable`,
  `wraps` (for the decorator), `Any`/`TypeVar`, and `RetryExhausted`.
- **Line 19** — `T = TypeVar("T")`: lets `retry_call` be generic — it returns whatever `fn` returns.
- **Lines 24–29** — `_compute_delay`: exponential backoff `base * 2**attempt`, capped at `cap` (26);
  apply symmetric jitter `±jitter` (27–28); floor at 0 (29). **Bug (adversarial #9):** the cap is
  applied *after* `2**attempt`, so a very large `attempt` overflows float conversion — should be
  `base * 2**min(attempt, ceiling)`.
- **Lines 32–45** — `retry_call` signature: `fn` then `*args`; keyword-only `on` (the exception tuple to
  catch), the backoff knobs, the injectable `sleeper`/`rng`, `wrap_final`, optional `log_extra`, and
  `**kwargs` forwarded to `fn`. Returns `T`.
- **Lines 51–52** — guard: `attempts < 1` is a programming error → `ValueError` (not an `EtlError`).
- **Line 53** — default the RNG if none injected.
- **Line 54** — track the last exception so we can re-raise it.
- **Lines 55–57** — loop up to `attempts`; try to call `fn` and return on success (the happy path costs
  zero sleeps).
- **Lines 58–61** — catch only the exceptions in `on`; remember it; if this was the last attempt, break
  out to the raise.
- **Lines 62–73** — otherwise compute the delay, log a structured warning (note `getattr(fn,
  "__name__", repr(fn))` handles callables without a name), and sleep via the injected `sleeper`.
- **Line 74** — `assert last is not None`: we only reach here after at least one caught exception, so
  `last` is set (also a type-narrowing hint for mypy).
- **Lines 75–77** — final behavior: `wrap_final=True` raises `RetryExhausted(attempts, last)` chained;
  otherwise re-raise the original exception unchanged (the default, so callers see the real error type).

```python
 80  def retry(
 81      *,
 82      on: tuple[type[BaseException], ...],
 83      attempts: int = 5,
 84      base: float = 1.0,
 85      cap: float = 30.0,
 86      jitter: float = 0.25,
 87      sleeper: Callable[[float], None] = time.sleep,
 88      rng: random.Random | None = None,
 89      wrap_final: bool = False,
 90  ) -> Callable[[Callable[..., T]], Callable[..., T]]:
 91      """Decorator form of `retry_call`."""
 92
 93      def decorator(fn: Callable[..., T]) -> Callable[..., T]:
 94          @wraps(fn)
 95          def wrapper(*args: Any, **kwargs: Any) -> T:
 96              return retry_call(
 97                  fn,
 98                  *args,
 99                  on=on,
 100                 attempts=attempts,
 101                 base=base,
 102                 cap=cap,
 103                 jitter=jitter,
 104                 sleeper=sleeper,
 105                 rng=rng,
 106                 wrap_final=wrap_final,
 107                 **kwargs,
 108             )
 109
 110         return wrapper
 111
 112     return decorator
```

**Line-by-line:**
- **Lines 80–91** — `retry`: the decorator factory. All args keyword-only; returns a decorator (a
  callable that takes a function and returns a function — note the nested `Callable[...]` return type).
- **Lines 93–108** — `decorator` receives the target `fn`; `@wraps(fn)` (94) preserves its name/docstring
  on the `wrapper`; `wrapper` simply forwards everything to `retry_call` with the captured knobs.
- **Lines 110, 112** — return the `wrapper`, then return the `decorator`. (The project mostly uses
  `retry_call` directly — e.g. the SFTP uploader — but the decorator is available and tested.)

## 14. `src/etl/validator.py`

Two-tier count validation: re-query `_count`, then a full re-extract, before giving up.

```python
 1  """Two-tier record-count validation.
 2
 3  Tier 1: re-query ES `_count` up to N times with exp backoff. Handles cases
 4          where a refresh races the initial count read.
 5  Tier 2: one full extract+CSV re-run via a caller-supplied callback.
 6
 7  If both tiers still disagree with the on-disk CSV row count, raise
 8  `RecordCountMismatch` with the full attempt history.
 9  """
10
11  from __future__ import annotations
12
13  import logging
14  import random
15  from collections.abc import Callable
16  from typing import Any
17
18  from etl.config import RetryConfig
19  from etl.errors import RecordCountMismatch
20  from etl.extractor import expected_count
21  from etl.models import CsvResult
22
23  _log = logging.getLogger(__name__)
24
25
26  def validate_counts(expected: int, actual: int) -> None:
27      if expected != actual:
28          raise RecordCountMismatch(expected=expected, actual=actual,
29                                    attempts=[(expected, actual)])
30
31
32  def validate_with_retry(
33      *,
34      es: Any,
35      index: str,
36      query: dict[str, Any],
37      csv_result: CsvResult,
38      retry_cfg: RetryConfig,
39      on_full_reextract: Callable[[], CsvResult],
40      sleeper: Callable[[float], None] | None = None,
41      rng: random.Random | None = None,
42      log_extra: dict[str, Any] | None = None,
43  ) -> CsvResult:
44      """Validate counts with the two-tier retry strategy.
45
46      Returns the `CsvResult` corresponding to the file that ultimately matched
47      (may be the re-extracted one).
48      """
49      import time as _time
50      sleeper = sleeper or _time.sleep
51      rng = rng or random.Random()
52      attempts_log: list[tuple[int, int]] = []
53      current = csv_result
54
55      # Tier 1: re-query _count up to N times.
56      for attempt in range(retry_cfg.max_attempts):
57          es_count = expected_count(es, index, query)
58          attempts_log.append((es_count, current.row_count))
59          if es_count == current.row_count:
60              return current
61          if attempt == retry_cfg.max_attempts - 1:
62              break
63          delay = min(retry_cfg.backoff_cap, retry_cfg.backoff_base * (2 ** attempt))
64          if retry_cfg.jitter > 0:
65              delay *= 1.0 + rng.uniform(-retry_cfg.jitter, retry_cfg.jitter)
66          _log.warning(
67              "count mismatch attempt=%d/%d es_count=%d csv_rows=%d delay=%.3fs",
68              attempt + 1, retry_cfg.max_attempts, es_count, current.row_count, delay,
69              extra={**(log_extra or {}), "retry_attempt": attempt + 1,
70                     "retry_delay_s": delay, "es_count": es_count,
71                     "csv_rows": current.row_count},
72          )
73          sleeper(max(0.0, delay))
74
75      # Tier 2: one full extract+CSV re-run.
76      _log.warning(
77          "count still mismatched after %d retries; running full re-extract",
78          retry_cfg.max_attempts, extra=log_extra,
79      )
80      current = on_full_reextract()
81      final_es_count = expected_count(es, index, query)
82      attempts_log.append((final_es_count, current.row_count))
83      if final_es_count != current.row_count:
84          raise RecordCountMismatch(
85              expected=final_es_count,
86              actual=current.row_count,
87              attempts=attempts_log,
88          )
89      return current
```

**Line-by-line:**
- **Lines 1–9** — docstring: tier 1 (re-`_count` N×, handles refresh races), tier 2 (one full
  re-extract); failure → `RecordCountMismatch` with the attempt history.
- **Lines 13–21** — imports: `logging`, `random`, `Callable`, `Any`, plus `RetryConfig`,
  `RecordCountMismatch`, `expected_count` (the `_count` call), and `CsvResult`.
- **Lines 26–29** — `validate_counts`: the simple one-shot check used in unit tests — raise if the two
  numbers differ.
- **Lines 32–43** — `validate_with_retry` signature: all keyword-only. Key parameter is
  `on_full_reextract` — a zero-arg callback the orchestrator supplies (inversion of control: the
  validator decides *when* to re-extract, the caller knows *how*). `sleeper`/`rng` injectable.
- **Lines 49–50** — local import of `time` and default the sleeper (kept local so tests overriding it
  stay clean).
- **Lines 52–53** — `attempts_log` accumulates every `(es_count, csv_rows)` pair; `current` tracks the
  CsvResult in play.
- **Lines 56–60** — **Tier 1 loop**: re-query `_count` (57), record the pair (58), and return
  immediately if it now matches the CSV row count (59–60) — the common "refresh had lagged" recovery.
- **Lines 61–62** — on the last attempt, stop looping (don't sleep after the final try).
- **Lines 63–65** — compute backoff with jitter inline (this loop retries on a *value comparison*, not
  an exception, which is why it doesn't reuse `retry_call`).
- **Lines 66–73** — log the mismatch with full context and sleep (`max(0.0, delay)` floors it).
- **Lines 76–80** — **Tier 2**: log, then call `on_full_reextract()` to rebuild the CSV from a fresh
  PIT; `current` becomes the new result.
- **Lines 81–82** — re-query `_count` once more and record the final pair.
- **Lines 83–88** — if it *still* disagrees, raise `RecordCountMismatch` with the entire `attempts_log`
  (fully auditable).
- **Line 89** — otherwise return the (re-extracted) `CsvResult` that matched.

## 15. `src/etl/extractor.py`

The thin seam that bridges the `etl` app to the standalone `es_extract` package.

```python
 1  """Bridges the ES client to the standalone extraction package.
 2
 3  The extractor is intentionally thin: it owns the ``_count`` call and the hit
 4  stream, delegating both to :mod:`es_extract` with failures pinned to
 5  :class:`etl.errors.ElasticsearchQueryError` so they land in the daemon's single
 6  ``EtlError`` boundary. Extraction paginates with point-in-time + ``search_after``.
 7  """
 8
 9  from __future__ import annotations
10
11  from collections.abc import Iterator
12  from typing import Any
13
14  from es_extract import count as _count
15  from es_extract import iter_hits as _iter_hits
16  from etl.errors import ElasticsearchQueryError
17  from etl.models import JobSpec
18
19
20  def expected_count(es: Any, index: str, query: dict[str, Any]) -> int:
21      return _count(es, index, query, error_cls=ElasticsearchQueryError)
22
23
24  def iter_hits(
25      es: Any,
26      job: JobSpec,
27      *,
28      page_size: int,
29      keep_alive: str = "5m",
30  ) -> Iterator[dict[str, Any]]:
31      """Stream a job's hits as ``_source`` dicts via PIT + ``search_after``."""
32      return _iter_hits(
33          es,
34          job.data_index,
35          job.query,
36          page_size=page_size,
37          keep_alive=keep_alive,
38          error_cls=ElasticsearchQueryError,
39      )
```

**Line-by-line:**
- **Lines 1–7** — docstring: this module is the *adapter*. It owns the count + the hit stream and pins
  failures to `ElasticsearchQueryError` so they fall under `EtlError`.
- **Lines 14–17** — imports: alias `es_extract.count`/`iter_hits` as private `_count`/`_iter_hits` (so
  this module can expose its own public `count`-like and `iter_hits` names without shadowing), plus the
  app's error type and `JobSpec`.
- **Lines 20–21** — `expected_count`: a one-liner over `_count`, injecting `error_cls=ElasticsearchQueryError`.
  This is the `_count` "ground truth" the validator uses.
- **Lines 24–30** — `iter_hits`: unpacks a `JobSpec` (its `data_index`/`query`) so callers pass a job,
  not loose fields. `page_size` is required (per-call), `keep_alive` defaults to "5m".
- **Lines 32–39** — delegate to the package's `_iter_hits`, again injecting `ElasticsearchQueryError`.
  Note it does **not** pass `source_only` → it takes the package default `True`, so the app gets
  `_source` dicts (the `_id` envelope is dropped here — the root of adversarial #8).

## 16. `src/etl/job_loader.py`

Resolves a `job_doc_id` to a validated `JobSpec` — the validate-at-the-boundary stage.

```python
 1  """Resolves a control message's `job_doc_id` to a `JobSpec` from Elasticsearch."""
 2
 3  from __future__ import annotations
 4
 5  from typing import Any
 6
 7  from etl.errors import ElasticsearchQueryError, JobSpecError
 8  from etl.models import JobSpec
 9
10  _REQUIRED = ("data_index", "query", "columns", "remote_filename")
11
12
13  def load_job(es: Any, *, job_index: str, job_doc_id: str) -> JobSpec:
14      try:
15          doc = es.get(index=job_index, id=job_doc_id)
16      except Exception as e:
17          raise ElasticsearchQueryError(
18              f"failed to GET job doc {job_doc_id} from {job_index}: {e!r}"
19          ) from e
20
21      if not doc.get("found", True):
22          raise JobSpecError(f"job doc {job_doc_id} not found in {job_index}")
23
24      source = doc.get("_source") or {}
25      missing = [f for f in _REQUIRED if f not in source]
26      if missing:
27          raise JobSpecError(f"job doc {job_doc_id} missing fields: {missing}")
28
29      query = source["query"]
30      if not isinstance(query, dict):
31          raise JobSpecError(f"job doc {job_doc_id}: 'query' must be an object")
32
33      columns = source["columns"]
34      if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
35          raise JobSpecError(f"job doc {job_doc_id}: 'columns' must be list[str]")
36
37      column_paths = source.get("column_paths", {})
38      if column_paths is None:
39          column_paths = {}
40      if not isinstance(column_paths, dict) or not all(
41          isinstance(k, str) and isinstance(v, str) for k, v in column_paths.items()
42      ):
43          raise JobSpecError(f"job doc {job_doc_id}: 'column_paths' must be dict[str, str]")
44
45      remote_filename = source["remote_filename"]
46      if not isinstance(remote_filename, str) or not remote_filename:
47          raise JobSpecError(f"job doc {job_doc_id}: 'remote_filename' must be non-empty string")
48
49      return JobSpec(
50          job_id=str(source.get("job_id", job_doc_id)),
51          data_index=str(source["data_index"]),
52          query=query,
53          column_paths=column_paths,
54          columns=list(columns),
55          remote_filename=remote_filename,
56      )
```

**Line-by-line:**
- **Line 1** — one-line docstring: id → `JobSpec` from ES.
- **Lines 7–8** — imports: the two error types it can raise, and `JobSpec`.
- **Line 10** — `_REQUIRED`: the field names the job document must contain.
- **Lines 13–19** — GET the document; wrap any client failure in `ElasticsearchQueryError` (the network
  failure path) — distinct from the *malformed-doc* path below.
- **Lines 21–22** — `doc.get("found", True)`: ES returns `found: false` for a missing id; treat that as
  a `JobSpecError` (a *data* problem, not a network one). Defaulting to `True` tolerates fakes that omit
  the key.
- **Lines 24–27** — pull `_source` (default `{}`), then compute which required fields are missing and
  raise listing them.
- **Lines 29–31** — type-check `query` must be a dict (an object), else `JobSpecError`.
- **Lines 33–35** — type-check `columns` must be a `list[str]`. **Gap (adversarial #6):** an *empty*
  list passes (`all(...)` is vacuously true), so a column-less job is accepted here and only fails later
  in `write_csv`.
- **Lines 37–43** — `column_paths` is optional: default `{}` (37), coerce explicit `None`→`{}` (38–39),
  then require a `dict[str, str]` (40–43).
- **Lines 45–47** — `remote_filename` must be a non-empty string.
- **Lines 49–56** — construct the trusted `JobSpec`. `job_id` defaults to `job_doc_id` if absent (50);
  `data_index` is coerced via `str()` (51); `columns` is copied with `list(...)` (54) to decouple from
  the source object. After this point, downstream code can assume shapes are correct.

## 17. `src/etl/control_consumer.py`

The Kafka control-topic wrapper: manual commits, poison handling, injectable consumer.

```python
  1  """Thin wrapper around `confluent_kafka.Consumer` for the control topic.
  2
  3  * `enable.auto.commit=False` — offsets are only committed via the explicit
  4    ack callback returned with each message.
  5  * Decodes message JSON into a `ControlMessage`. Malformed messages raise
  6    `ControlMessageError` so the orchestrator can decide whether to commit
  7    past the poison record or skip-and-alert.
  8  """
  9
 10  from __future__ import annotations
 11
 12  import json
 13  import logging
 14  from collections.abc import Callable, Iterator
 15  from typing import Any
 16
 17  from etl.config import KafkaConfig
 18  from etl.errors import ControlMessageError
 19  from etl.models import ControlMessage
 20
 21  _log = logging.getLogger(__name__)
 22
 23
 24  class ControlConsumer:
 25      def __init__(self, cfg: KafkaConfig, *, consumer_factory: Callable[[dict[str, str]], Any] | None = None) -> None:
 26          if consumer_factory is None:
 27              from confluent_kafka import Consumer  # local import — heavy dep
 28              consumer_factory = Consumer
 29          self._cfg = cfg
 30          self._consumer = consumer_factory(cfg.confluent_config())
 31          self._consumer.subscribe([cfg.control_topic])
 32
 33      @staticmethod
 34      def _decode(raw: bytes, *, partition: int, offset: int) -> ControlMessage:
 35          try:
 36              payload = json.loads(raw.decode("utf-8"))
 37          except (UnicodeDecodeError, json.JSONDecodeError) as e:
 38              raise ControlMessageError(f"undecodable control message: {e!r}") from e
 39          if not isinstance(payload, dict):
 40              raise ControlMessageError(f"control message must be a JSON object, got {type(payload).__name__}")
 41          job_doc_id = payload.get("job_doc_id") or payload.get("id")
 42          if not isinstance(job_doc_id, str) or not job_doc_id:
 43              raise ControlMessageError("control message missing required 'job_doc_id'")
 44          correlation_id = payload.get("correlation_id")
 45          if correlation_id is not None and not isinstance(correlation_id, str):
 46              raise ControlMessageError("'correlation_id' must be string or absent")
 47          return ControlMessage(
 48              job_doc_id=job_doc_id,
 49              correlation_id=correlation_id,
 50              raw_partition=partition,
 51              raw_offset=offset,
 52          )
 53
 54      def iter_messages(
 55          self,
 56          *,
 57          poll_timeout_s: float = 1.0,
 58          stop: Callable[[], bool] | None = None,
 59      ) -> Iterator[tuple[ControlMessage, Callable[[], None], Any]]:
 60          """Yield `(ControlMessage, commit_fn, raw_kafka_msg)` tuples.
 61
 62          `commit_fn()` commits the offset for that message synchronously. Call
 63          it only after the job has been processed successfully — failures
 64          should leave the offset unmoved so the message is redelivered.
 65          """
 66          while True:
 67              if stop is not None and stop():
 68                  return
 69              msg = self._consumer.poll(timeout=poll_timeout_s)
 70              if msg is None:
 71                  continue
 72              if msg.error():
 73                  _log.warning("kafka poll error: %r", msg.error())
 74                  continue
 75              # partition()/offset() are typed Optional but are always present on
 76              # a fetched record; coalesce to satisfy the type checker.
 77              partition = int(msg.partition() or 0)
 78              offset = int(msg.offset() or 0)
 79              value = msg.value()
 80              if value is None:
 81                  # Null-valued record (e.g. a tombstone): nothing to act on.
 82                  _log.warning("null control message value at p=%s o=%s; skipping",
 83                               partition, offset)
 84                  self._consumer.commit(message=msg, asynchronous=False)
 85                  continue
 86              raw = value.encode("utf-8") if isinstance(value, str) else value
 87              try:
 88                  ctrl = self._decode(raw, partition=partition, offset=offset)
 89              except ControlMessageError as e:
 90                  _log.error("poison control message at p=%s o=%s: %r", partition, offset, e)
 91                  # Skip past it so the daemon doesn't loop on poison forever.
 92                  self._consumer.commit(message=msg, asynchronous=False)
 93                  continue
 94
 95              def _commit(_m: Any = msg) -> None:
 96                  self._consumer.commit(message=_m, asynchronous=False)
 97
 98              yield ctrl, _commit, msg
 99
 100     def close(self) -> None:
 101         try:
 102             self._consumer.close()
 103         except Exception as e:
 104             _log.warning("consumer close failed: %r", e)
```

**Line-by-line:**
- **Lines 1–8** — docstring: manual commit only; malformed → `ControlMessageError` so the loop can
  decide poison handling.
- **Lines 12–19** — imports: `json`, `logging`, `Callable`/`Iterator`, `Any`, plus `KafkaConfig`, the
  error, and `ControlMessage`.
- **Line 25** — constructor with an **injectable `consumer_factory`** (the key test seam) defaulting to
  `None`.
- **Lines 26–28** — if no factory given, lazily import the heavy `confluent_kafka.Consumer` (keeps the
  import cost out of unit tests and other importers).
- **Lines 29–31** — store config, build the consumer from `cfg.confluent_config()`, and subscribe to the
  control topic.
- **Lines 33–34** — `_decode` is a `@staticmethod` (no instance state needed) turning raw bytes into a
  `ControlMessage`.
- **Lines 35–38** — parse JSON from UTF-8; both decode and JSON errors become `ControlMessageError`.
- **Lines 39–40** — reject non-object payloads (e.g. a JSON array or scalar) — the type name is included
  for diagnosis.
- **Lines 41–43** — accept `job_doc_id` or legacy `id`; require a non-empty string.
- **Lines 44–46** — `correlation_id` is optional but, if present, must be a string.
- **Lines 47–52** — build the immutable `ControlMessage`, stamping the partition/offset for the later
  exact commit.
- **Lines 54–59** — `iter_messages`: a generator yielding `(ctrl, commit_fn, raw_msg)`. `poll_timeout_s`
  bounds each poll; `stop` is a callback the daemon uses for graceful shutdown.
- **Lines 66–68** — top of loop: check the stop flag first so a shutdown is honored before blocking on a
  poll.
- **Lines 69–71** — poll; `None` means "no message this interval" → loop again.
- **Lines 72–74** — a broker-level error on the record is logged and skipped.
- **Lines 75–78** — read partition/offset, coalescing `None`→0 (the comment notes they're typed
  Optional but always present on a fetched record).
- **Lines 79–85** — a null value (Kafka tombstone) has nothing to process: log, **commit past it**, and
  continue.
- **Line 86** — normalize the value to bytes (some clients hand back `str`).
- **Lines 87–93** — decode; on `ControlMessageError` (poison), log, **commit past it** so the daemon
  doesn't loop forever on the same bad record, and continue.
- **Lines 95–96** — build the `_commit` closure. The `_m=msg` default argument **binds the current
  message** at definition time — crucial in a loop, so the closure commits *this* message, not whatever
  `msg` later becomes. Commit is synchronous (`asynchronous=False`).
- **Line 98** — yield the tuple; the caller drives processing and decides when to call `_commit`.
- **Lines 100–104** — `close` shuts the consumer down best-effort (a close failure is logged, not
  raised, so shutdown can't be derailed).

## 18. `src/etl/sftp_uploader.py`

Delivery via the system `sftp` binary, hardened and retried.

```python
  1  """SFTP upload via the system `sftp` binary.
  2
  3  We intentionally shell out (no `paramiko`) and force strict host-key checking
  4  against a user-supplied `known_hosts` file. The batch file is written into a
  5  temp dir, used with `-b`, then removed.
  6  """
  7
  8  from __future__ import annotations
  9
 10  import logging
 11  import shlex
 12  import subprocess
 13  import tempfile
 14  import time
 15  from collections.abc import Callable
 16  from dataclasses import dataclass
 17  from pathlib import Path
 18
 19  from etl.config import RetryConfig, SftpConfig
 20  from etl.errors import SftpUploadError
 21  from etl.retry import retry_call
 22
 23  _log = logging.getLogger(__name__)
 24
 25
 26  @dataclass(frozen=True)
 27  class UploadPlan:
 28      """Pairs each local file with its remote destination path."""
 29
 30      local: Path
 31      remote: str
 32
 33
 34  def _build_batch(plans: list[UploadPlan]) -> str:
 35      lines: list[str] = []
 36      for p in plans:
 37          # `sftp` batch files use whitespace as the separator. The shlex.quote
 38          # call protects against spaces in paths; bare metacharacters in
 39          # filenames are otherwise harmless here (no shell involved).
 40          lines.append(f"put {shlex.quote(str(p.local))} {shlex.quote(p.remote)}")
 41      lines.append("bye")
 42      return "\n".join(lines) + "\n"
 43
 44
 45  def _run_sftp(cfg: SftpConfig, batch_text: str, *, timeout: float) -> None:
 46      with tempfile.NamedTemporaryFile(
 47          mode="w", suffix=".sftpbatch", delete=True, encoding="utf-8"
 48      ) as batch_fh:
 49          batch_fh.write(batch_text)
 50          batch_fh.flush()
 51          argv = [
 52              "sftp",
 53              "-b", batch_fh.name,
 54              "-i", str(cfg.key_path),
 55              "-P", str(cfg.port),
 56              "-o", f"UserKnownHostsFile={cfg.known_hosts}",
 57              "-o", "StrictHostKeyChecking=yes",
 58              "-o", "BatchMode=yes",
 59              f"{cfg.user}@{cfg.host}",
 60          ]
 61          _log.info("sftp invoking", extra={"argv": argv})
 62          try:
 63              proc = subprocess.run(
 64                  argv,
 65                  check=False,
 66                  capture_output=True,
 67                  timeout=timeout,
 68              )
 69          except subprocess.TimeoutExpired as e:
 70              raise SftpUploadError(f"sftp timed out after {timeout}s: {e!r}") from e
 71          except FileNotFoundError as e:
 72              raise SftpUploadError(f"sftp binary not found: {e!r}") from e
 73          if proc.returncode != 0:
 74              raise SftpUploadError(
 75                  f"sftp exit={proc.returncode} stderr={proc.stderr.decode('utf-8', 'replace')!r}"
 76              )
 77
 78
 79  def upload(
 80      cfg: SftpConfig,
 81      plans: list[UploadPlan],
 82      *,
 83      retry_cfg: RetryConfig,
 84      timeout: float = 300.0,
 85      sleeper: Callable[[float], None] | None = None,
 86  ) -> None:
 87      """Upload a set of files via sftp with retry-on-failure."""
 88      batch_text = _build_batch(plans)
 89      retry_call(
 90          _run_sftp,
 91          cfg,
 92          batch_text,
 93          timeout=timeout,
 94          on=(SftpUploadError,),
 95          attempts=retry_cfg.max_attempts,
 96          base=retry_cfg.backoff_base,
 97          cap=retry_cfg.backoff_cap,
 98          jitter=retry_cfg.jitter,
 99          sleeper=sleeper if sleeper is not None else time.sleep,
 100     )
```

**Line-by-line:**
- **Lines 1–6** — docstring: why subprocess over `paramiko`, strict host-key checking, temp batch file.
- **Lines 10–21** — imports: `shlex` (quoting), `subprocess` (the binary), `tempfile` (the batch file),
  `time` (default sleeper), `Callable`/`dataclass`/`Path`, plus the configs, error, and `retry_call`.
- **Lines 26–31** — `UploadPlan`: a frozen pair of `local: Path` and `remote: str` (one per file).
- **Lines 34–42** — `_build_batch`: build the `sftp -b` script. Each line is `put <local> <remote>` with
  both paths `shlex.quote`d for spaces (37–40); end with `bye` (41); join with newlines (42). The
  comment notes no shell is involved, so metacharacters are harmless.
- **Lines 45–48** — `_run_sftp`: write the batch into a `NamedTemporaryFile` (`delete=True` auto-removes
  it on context exit, even on error).
- **Lines 49–50** — write and `flush()` so the bytes are on disk before `sftp` reads the file by name.
- **Lines 51–60** — build the **argv list** (no `shell=True`, so no injection): the binary, `-b` batch
  file, `-i` key, `-P` port, and three `-o` options — `UserKnownHostsFile` (the operator's pinned
  hosts), `StrictHostKeyChecking=yes` (refuse unknown hosts — defeats MITM), `BatchMode=yes` (never
  prompt; fail instead of hanging) — then `user@host`.
- **Line 61** — log the exact argv (this is the line we saw in the live smoke test).
- **Lines 62–68** — run the process: `check=False` (we inspect the return code ourselves),
  `capture_output=True` (grab stderr for the error message), `timeout` (bound a stalled transfer).
- **Lines 69–72** — translate a timeout and a missing-binary into `SftpUploadError` (chained).
- **Lines 73–76** — a non-zero exit becomes `SftpUploadError`, embedding the decoded stderr
  (`errors='replace'` so undecodable bytes don't crash the error path).
- **Lines 79–86** — `upload`: the public entry. `timeout` defaults to 5 minutes; `sleeper` injectable.
- **Line 88** — build the batch once (so retries reuse the same plan).
- **Lines 89–99** — drive `_run_sftp` through `retry_call`, retrying **only** on `SftpUploadError`
  (line 94) with the configured backoff. This is the retry layer the failure-mode smoke test exercises.

> **Open item (REVIEW §3.3):** `_build_batch` `put`s straight to the final remote name — no
> upload-to-temp-then-rename — so a partner polling the directory can see a half-written file. The
> CSV-before-sidecar ordering is the implicit "ready" signal; documenting or hardening it is a ticket.

## 19. `src/etl/pipeline.py`

`run_one` — composition of every stage for a single control message. It does **not** commit.

```python
  1  """End-to-end orchestration for one control message.
  2
  3  `run_one` is the single entry point. It does *not* commit offsets — the
  4  caller (`__main__`) holds the commit decision so a failing job leaves the
  5  offset unmoved (the control message is redelivered on the next poll).
  6  """
  7
  8  from __future__ import annotations
  9
 10  import logging
 11  from pathlib import Path
 12  from typing import Any
 13
 14  from es_extract.diagnostics import tee_to_ndjson
 15  from etl.config import Settings
 16  from etl.csv_writer import write_csv
 17  from etl.extractor import expected_count, iter_hits
 18  from etl.job_loader import load_job
 19  from etl.models import ControlMessage, CsvResult, JobSpec
 20  from etl.sftp_uploader import UploadPlan, upload
 21  from etl.transformer import iter_transformed
 22  from etl.validator import validate_with_retry
 23
 24  _log = logging.getLogger(__name__)
 25
 26
 27  def _staged_paths(csv_dir: Path, job: JobSpec) -> tuple[Path, str, str]:
 28      """Local staging path for the CSV + remote target paths for csv & sidecar."""
 29      local_basename = Path(job.remote_filename).name
 30      local_csv = csv_dir / local_basename
 31      remote_csv = job.remote_filename
 32      remote_sidecar = remote_csv + ".sha256"
 33      return local_csv, remote_csv, remote_sidecar
 34
 35
 36  def _do_extract_to_csv(
 37      *,
 38      es: Any,
 39      job: JobSpec,
 40      page_size: int,
 41      keep_alive: str,
 42      local_csv: Path,
 43      raw_dump_path: Path | None = None,
 44  ) -> CsvResult:
 45      hits = iter_hits(es, job, page_size=page_size, keep_alive=keep_alive)
 46      if raw_dump_path is not None:
 47          # Diagnostic: tee the raw extracted hits to NDJSON as they stream.
 48          hits = tee_to_ndjson(hits, raw_dump_path)
 49      rows = iter_transformed(hits, job.column_paths, job.columns, job_id=job.job_id)
 50      return write_csv(rows, job.columns, local_csv)
 51
 52
 53  def run_one(
 54      *,
 55      ctrl: ControlMessage,
 56      es: Any,
 57      settings: Settings,
 58  ) -> None:
 59      log_extra = {
 60          "job_doc_id": ctrl.job_doc_id,
 61          "correlation_id": ctrl.correlation_id,
 62          "kafka_partition": ctrl.raw_partition,
 63          "kafka_offset": ctrl.raw_offset,
 64      }
 65      _log.info("loading job", extra=log_extra)
 66      job = load_job(es, job_index=settings.es.job_index, job_doc_id=ctrl.job_doc_id)
 67      log_extra["job_id"] = job.job_id
 68      log_extra["data_index"] = job.data_index
 69
 70      initial_count = expected_count(es, job.data_index, job.query)
 71      _log.info("expected_count=%d", initial_count, extra=log_extra)
 72
 73      local_csv, remote_csv, remote_sidecar = _staged_paths(settings.csv_output_dir, job)
 74
 75      raw_dump_path = (
 76          settings.raw_dump_dir / f"{job.job_id}.ndjson"
 77          if settings.raw_dump_dir is not None
 78          else None
 79      )
 80      if raw_dump_path is not None:
 81          _log.info("raw hit dump enabled path=%s", raw_dump_path, extra=log_extra)
 82
 83      csv_result = _do_extract_to_csv(
 84          es=es,
 85          job=job,
 86          page_size=settings.pagination.page_size,
 87          keep_alive=settings.pagination.pit_keep_alive,
 88          local_csv=local_csv,
 89          raw_dump_path=raw_dump_path,
 90      )
 91      _log.info("csv written rows=%d sha256=%s",
 92                csv_result.row_count, csv_result.sha256_hex, extra=log_extra)
 93
 94      def _reextract() -> CsvResult:
 95          _log.warning("re-extracting after count mismatch", extra=log_extra)
 96          # A fresh call opens a new point-in-time; the previous one is spent.
 97          return _do_extract_to_csv(
 98              es=es, job=job,
 99              page_size=settings.pagination.page_size,
 100             keep_alive=settings.pagination.pit_keep_alive,
 101             local_csv=local_csv,
 102             raw_dump_path=raw_dump_path,
 103         )
 104
 105     csv_result = validate_with_retry(
 106         es=es,
 107         index=job.data_index,
 108         query=job.query,
 109         csv_result=csv_result,
 110         retry_cfg=settings.retry,
 111         on_full_reextract=_reextract,
 112         log_extra=log_extra,
 113     )
 114     _log.info("counts validated rows=%d", csv_result.row_count, extra=log_extra)
 115
 116     upload(
 117         settings.sftp,
 118         [
 119             UploadPlan(local=csv_result.csv_path, remote=remote_csv),
 120             UploadPlan(local=csv_result.sidecar_path, remote=remote_sidecar),
 121         ],
 122         retry_cfg=settings.retry,
 123     )
 124     _log.info("upload complete remote=%s", remote_csv, extra=log_extra)
```

**Line-by-line:**
- **Lines 1–6** — docstring: the offset-commit decision lives in `__main__`, not here — so a failure
  leaves the offset put and the message is redelivered.
- **Lines 14–22** — imports: every stage function (`load_job`, `expected_count`/`iter_hits`,
  `iter_transformed`, `write_csv`, `validate_with_retry`, `upload`), the `tee_to_ndjson` diagnostic,
  `Settings`, and the models.
- **Lines 27–33** — `_staged_paths`: derive the local staging path (the basename of `remote_filename`
  under `csv_output_dir`, line 29–30) plus the remote CSV path and `<remote>.sha256` sidecar path.
- **Lines 36–44** — `_do_extract_to_csv`: all keyword-only; `raw_dump_path` optional.
- **Line 45** — start the lazy hit stream from the extractor.
- **Lines 46–48** — if a raw dump is configured, wrap the stream in `tee_to_ndjson` (the hits are still
  streamed; they're now also recorded as they pass).
- **Line 49** — wrap again in `iter_transformed` (projection). Still lazy — nothing has run yet.
- **Line 50** — `write_csv` is the **terminal** stage that actually pulls the chain and returns a
  `CsvResult`. This is where extract→tee→transform→write all execute, one row at a time.
- **Lines 53–58** — `run_one`: keyword-only `ctrl`, `es` (injected — testability), `settings`. Returns
  `None` (its effect is the delivered file).
- **Lines 59–64** — build `log_extra` with the Kafka context so every subsequent log line is tagged.
- **Lines 65–68** — load the job; enrich `log_extra` with `job_id`/`data_index` once known.
- **Lines 70–71** — the up-front `_count`, logged as `expected_count`. **(REVIEW §3.6:** this is
  *logged only* — the validator re-queries `_count` itself, so this initial call is observability, not
  the validation source of truth.)
- **Line 73** — compute the staged + remote paths.
- **Lines 75–81** — build the optional raw-dump path (`<dir>/<job_id>.ndjson`) and log if enabled.
- **Lines 83–92** — run the extract→CSV chain and log the row count + digest (the `csv written` line
  from the live smoke test).
- **Lines 94–103** — `_reextract`: a **closure** capturing `es`/`job`/`settings`/paths. It's handed to
  the validator as a zero-arg callable; each call opens a **fresh PIT** (the previous is spent) and
  overwrites the same local path (idempotent).
- **Lines 105–114** — validate with the two-tier strategy, passing the `_reextract` callback; the
  returned `csv_result` is whichever file ultimately matched.
- **Lines 116–123** — upload **both** files in one batch: the CSV first, then the sidecar (the ordering
  that serves as the "ready" signal). Retries come from `retry_cfg`.
- **Line 124** — final success log. Note there is **no commit here** — that's the caller's job.

## 20. `src/etl/__main__.py`

The daemon: `python -m etl`. The poll loop, the commit decision, and graceful shutdown.

```python
  1  """Daemon entry point — `python -m etl`.
  2
  3  Polls the control topic in a loop, runs one job per message, and commits the
  4  offset only after the job succeeds. Job-scoped errors (`EtlError`) are
  5  logged and the loop continues. Anything else propagates and exits non-zero.
  6  """
  7
  8  from __future__ import annotations
  9
 10  import contextlib
 11  import logging
 12  import signal
 13  import sys
 14  from types import FrameType
 15  from typing import Any
 16
 17  from etl.config import Settings, load_settings
 18  from etl.control_consumer import ControlConsumer
 19  from etl.errors import ConfigError, EtlError
 20  from etl.logging_setup import configure_logging
 21  from etl.pipeline import run_one
 22
 23  _log = logging.getLogger("etl.main")
 24
 25
 26  def _build_es_client(settings: Settings) -> Any:
 27      """Construct the official `elasticsearch.Elasticsearch` client."""
 28      from elasticsearch import Elasticsearch  # local import — heavy dep
 29
 30      kwargs: dict[str, Any] = {"hosts": settings.es.hosts}
 31      if settings.es.api_key:
 32          kwargs["api_key"] = settings.es.api_key
 33      elif settings.es.username and settings.es.password:
 34          kwargs["basic_auth"] = (settings.es.username, settings.es.password)
 35      return Elasticsearch(**kwargs)
 36
 37
 38  def main() -> int:
 39      try:
 40          settings = load_settings()
 41      except ConfigError as e:
 42          # Logging may not be configured yet; print and exit.
 43          print(f"config error: {e}", file=sys.stderr)
 44          return 2
 45
 46      configure_logging(settings.log_level)
 47      _log.info("starting etl daemon", extra={"control_topic": settings.kafka.control_topic})
 48
 49      stopping = {"flag": False}
 50
 51      def _on_signal(signum: int, _frame: FrameType | None) -> None:
 52          _log.info("received signal %s; stopping", signum)
 53          stopping["flag"] = True
 54
 55      signal.signal(signal.SIGINT, _on_signal)
 56      signal.signal(signal.SIGTERM, _on_signal)
 57
 58      es = _build_es_client(settings)
 59      consumer = ControlConsumer(settings.kafka)
 60
 61      exit_code = 0
 62      try:
 63          for ctrl, commit, _raw in consumer.iter_messages(stop=lambda: stopping["flag"]):
 64              try:
 65                  run_one(ctrl=ctrl, es=es, settings=settings)
 66              except EtlError as e:
 67                  # The job already exhausted its internal retries (counts, SFTP)
 68                  # before raising. Halt without committing so the offset stays
 69                  # put and this exact message is redelivered on the next start.
 70                  # We must NOT `continue`: advancing to the next message and
 71                  # committing its offset would commit *over* this failed one
 72                  # (Kafka offsets are "up to and including"), silently dropping it.
 73                  _log.error(
 74                      "job failed; halting without commit so it is redelivered: %r",
 75                      e,
 76                      extra={
 77                          "job_doc_id": ctrl.job_doc_id,
 78                          "correlation_id": ctrl.correlation_id,
 79                      },
 80                  )
 81                  exit_code = 1
 82                  break
 83              commit()
 84              _log.info("job committed", extra={"job_doc_id": ctrl.job_doc_id})
 85      finally:
 86          consumer.close()
 87          with contextlib.suppress(Exception):  # pragma: no cover - best effort
 88              es.close()
 89
 90      return exit_code
 91
 92
 93  if __name__ == "__main__":
 94      raise SystemExit(main())
```

**Line-by-line:**
- **Lines 1–6** — docstring: the loop's contract — commit only after success; `EtlError` logged and the
  loop *halts* (see below); anything else propagates.
- **Lines 10–21** — imports: `contextlib` (suppress on cleanup), `signal`, `sys`, `FrameType` (signal
  handler typing), plus the config loader, consumer, errors, logging setup, and `run_one`.
- **Line 23** — logger explicitly named `"etl.main"` (matches the `logger` field we saw in the smoke log).
- **Lines 26–35** — `_build_es_client`: lazy `Elasticsearch` import (28); build kwargs from config —
  `hosts` always, then `api_key` if set, else `basic_auth` if username+password present (31–34);
  construct and return the client.
- **Lines 38–44** — `main` begins by loading settings. A `ConfigError` is special: logging may not be
  configured yet, so print to stderr and **exit 2** *before* the loop (this is the one `EtlError`
  subclass handled out-of-band).
- **Lines 46–47** — configure JSON logging at the configured level, then the `starting etl daemon` line.
- **Lines 49–56** — graceful-shutdown plumbing: a mutable `stopping` flag (dict so the closure can
  mutate it), a signal handler that flips it, registered for SIGINT and SIGTERM. The loop checks the
  flag — we never abort mid-job.
- **Lines 58–59** — build the ES client and the consumer.
- **Line 61** — default exit code 0 (clean).
- **Lines 62–63** — the loop, driven by `iter_messages` with the stop callback wired to the flag.
- **Lines 64–65** — process one message via `run_one`.
- **Lines 66–82** — **the critical correctness block.** On `EtlError` (the job already exhausted its
  internal retries), log the failure and **`break`** — do *not* `continue`. The comment spells out why:
  Kafka offsets are cumulative ("up to and including"), so committing a later message would commit over
  the failed one and silently drop it. Halting leaves the offset put → redelivery on restart. Sets
  `exit_code = 1`.
- **Lines 83–84** — only reached on success: `commit()` the offset, then log `job committed` (the last
  line of the live smoke trace).
- **Lines 85–88** — `finally`: always close the consumer; close the ES client best-effort
  (`contextlib.suppress(Exception)` so a close failure can't mask the real outcome; `# pragma: no
  cover` excludes it from coverage).
- **Line 90** — return the exit code to the OS.
- **Lines 93–94** — the module-run guard: `raise SystemExit(main())` so `python -m etl` exits with
  `main`'s return code.

---

# Part III — packaging & config

## 21. `pyproject.toml`

```toml
 1  [build-system]
 2  requires = ["setuptools>=68", "wheel"]
 3  build-backend = "setuptools.build_meta"
 4
 5  [project]
 6  name = "kafka-es-csv-sftp-etl"
 7  version = "0.1.0"
 8  description = "Control-driven ETL: Kafka control topic -> Elasticsearch -> dotted-path projection -> CSV -> SFTP"
 9  requires-python = ">=3.10"
10  dependencies = [
11      "confluent-kafka>=2.3",
12      "elasticsearch>=8.0,<9",
13      "python-dotenv>=1.0",
14  ]
15
16  [project.optional-dependencies]
17  dev = [
18      "pytest>=7.4",
19      "pytest-mock>=3.12",
20      "pytest-cov>=4.1",
21      "ruff>=0.4",
22      "mypy>=1.8",
23  ]
24
25  [project.scripts]
26  etl = "etl.__main__:main"
27
28  [tool.setuptools.packages.find]
29  where = ["src"]
30
31  [tool.pytest.ini_options]
32  testpaths = ["tests"]
33  addopts = "-ra -q"
34  pythonpath = ["src"]
35
36  [tool.ruff]
37  line-length = 100
38  target-version = "py310"
39  src = ["src", "tests"]
40
41  [tool.ruff.lint]
42  select = ["E", "F", "I", "B", "UP", "SIM", "RUF"]
43  ignore = ["E501"]
44
45  [tool.mypy]
46  python_version = "3.10"
47  strict = true
48  packages = ["etl", "es_extract"]
49  mypy_path = "src"
```

**Line-by-line:**
- **Lines 1–3** — build backend: setuptools ≥68 + wheel, the standard PEP 517 setup.
- **Lines 5–9** — project identity. The `description` (line 8) was corrected to say "dotted-path
  projection" (was "JOLT" — REVIEW R3). `requires-python = ">=3.10"` matches the `str | None` syntax
  used throughout.
- **Lines 10–14** — runtime deps: `confluent-kafka`, `elasticsearch>=8.0,<9` (the **`<9` cap is
  deliberate** — 9.x removes the `body=` argument the code uses), and `python-dotenv`.
- **Lines 16–23** — the `dev` optional group: pytest (+ mock, + cov), ruff, mypy. Installed via
  `pip install -e ".[dev]"`.
- **Lines 25–26** — a console entry point: `etl` → `etl.__main__:main`, so the daemon can run as `etl`
  (in addition to `python -m etl`).
- **Lines 28–29** — src-layout discovery: packages are found under `src/` (so `etl` and `es_extract` are
  importable only when installed, not by accident from the cwd).
- **Lines 31–34** — pytest config: tests live in `tests/`, `-ra -q` (concise output + a report of
  non-passing), and `pythonpath=["src"]` so the suite imports the packages without an editable install.
- **Lines 36–43** — ruff: 100-col lines, py310 target, lints `src`+`tests`. The selected rule families
  (42): `E`/`F` (pycodestyle/pyflakes), `I` (import sort), `B` (bugbear), `UP` (pyupgrade), `SIM`
  (simplify), `RUF` (ruff-specific). `E501` (line length) is ignored (43) since `line-length` already
  governs formatting.
- **Lines 45–49** — mypy: **`strict = true`** over both packages, with `mypy_path = "src"` so it resolves
  the src-layout. Strict mode is what forces the full annotations seen in every module.

## 22. `requirements.txt` and `requirements-dev.txt`

```text
# requirements.txt
confluent-kafka>=2.3
elasticsearch>=8.0,<9
python-dotenv>=1.0
```

```text
# requirements-dev.txt
-r requirements.txt
pytest>=7.4
pytest-mock>=3.12
pytest-cov>=4.1
ruff>=0.4
mypy>=1.8
```

**Notes:** both files are **convenience mirrors** of `pyproject.toml` (each carries a header saying so)
for tools/pipelines that expect a `requirements.txt`. `requirements.txt` lists exactly the three runtime
deps; `requirements-dev.txt` pulls those in via `-r requirements.txt` and adds the five dev tools. The
canonical install remains `pip install -e ".[dev]"` — if you change a dependency, change it in
`pyproject.toml` and keep these in sync.

## 23. `.env.example`

```bash
 1  # Kafka
 2  KAFKA_BOOTSTRAP_SERVERS=localhost:9092
 3  KAFKA_CONTROL_TOPIC=etl.control
 4  KAFKA_GROUP_ID=etl-runner
 5  # Optional SASL/SSL
 6  # KAFKA_SECURITY_PROTOCOL=SASL_SSL
 7  # KAFKA_SASL_MECHANISM=PLAIN
 8  # KAFKA_SASL_USERNAME=
 9  # KAFKA_SASL_PASSWORD=
10
11  # Elasticsearch
12  ES_HOSTS=http://localhost:9200
13  # Pick one auth style:
14  # ES_USERNAME=elastic
15  # ES_PASSWORD=changeme
16  # ES_API_KEY=
17  ES_JOB_INDEX=etl-jobs
18
19  # Pagination (point-in-time + search_after)
20  PAGE_SIZE=1000
21  PIT_KEEP_ALIVE=5m
22
23  # Retry knobs
24  RETRY_MAX_ATTEMPTS=5
25  RETRY_BACKOFF_BASE=1.0
26  RETRY_BACKOFF_CAP=30.0
27  RETRY_JITTER=0.25
28
29  # Local staging
30  CSV_OUTPUT_DIR=/tmp/etl-csv
31
32  # Diagnostics (optional). When set, each job tees its raw extracted hits to
33  # <ES_RAW_DUMP_DIR>/<job_id>.ndjson as they stream. Unset = disabled.
34  # ES_RAW_DUMP_DIR=/tmp/etl-raw
35
36  # SFTP
37  SFTP_HOST=sftp.example.com
38  SFTP_PORT=22
39  SFTP_USER=etl
40  SFTP_KEY_PATH=/home/etl/.ssh/id_ed25519
41  SFTP_REMOTE_DIR=/incoming
42  SFTP_KNOWN_HOSTS=/home/etl/.ssh/known_hosts
43
44  # Logging
45  LOG_LEVEL=INFO
```

**Notes:** the template documents **every** variable `load_settings` reads, grouped by subsystem.
Required vars have concrete values; optional ones (SASL/SSL, ES auth, `ES_RAW_DUMP_DIR`) are commented
out so they default to `None`/disabled. Each line maps directly to a `config.py` read — e.g. line 20
→ `_get_int("PAGE_SIZE", 1000)` (line 144 there). The pagination block (19–21) is **PIT-only** (the old
`PAGINATION_STRATEGY`/`SCROLL_KEEP_ALIVE` were removed in the refactor). Copy to `.env` to run.

## 24. `.gitignore`

```gitignore
 1  __pycache__/
 2  *.py[cod]
 3  *.egg-info/
 4  .eggs/
 5  build/
 6  dist/
 7
 8  .venv/
 9  venv/
10  .env
11  .env.local
12
13  .pytest_cache/
14  .mypy_cache/
15  .ruff_cache/
16  .coverage
17  htmlcov/
18
19  .DS_Store
20  .idea/
21  .vscode/
22
23  # Local mock environment artifacts (generated by scripts/setup_local.sh)
24  local/keys/
25  local/sftp/
```

**Notes:**
- **Lines 1–6** — Python build/bytecode artifacts (`__pycache__`, `.pyc`/`.pyo`, egg-info, build/dist).
- **Lines 8–11** — virtualenvs and the **secret-bearing env files** (`.env`, `.env.local`). This is why
  the `.env.local` edit made earlier in the session is *not* committed — git ignores it.
- **Lines 13–17** — tool caches and coverage output (pytest/mypy/ruff caches, `.coverage`, `htmlcov`).
- **Lines 19–21** — OS/editor cruft.
- **Lines 23–25** — the **generated mock artifacts** from `setup_local.sh`: `local/keys/` (the ETL
  client keypair + captured `known_hosts`) and `local/sftp/` (host keys, authorized_keys, the upload
  drop dir). Throwaway, never committed.

---

---

# Part IV — the tests (`tests/`)

The whole suite is **hermetic**: no real Kafka/ES/SFTP, no network, no Docker. Every external boundary
is a fake or a monkeypatch. Read this part with §B.4 of `REVIEW.v3.md` (the index) for the big picture.

## 25. `tests/conftest.py` — shared fixtures

```python
  1  """Shared pytest fixtures.
  2
  3  Note: every fixture here is a pure-Python fake. No real Kafka/ES/SFTP
  4  clients are imported, so the test suite runs without network or Docker.
  5  """
  6
  7  from __future__ import annotations
  8
  9  from pathlib import Path
 10  from typing import Any
 11
 12  import pytest
 13
 14  from etl.config import (
 15      EsConfig,
 16      KafkaConfig,
 17      PaginationConfig,
 18      RetryConfig,
 19      Settings,
 20      SftpConfig,
 21  )
 22  from etl.models import JobSpec
 23
 24
 25  @pytest.fixture(autouse=True)
 26  def _isolate_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
 27      """Keep config tests hermetic. ..."""
 28      monkeypatch.setattr("etl.config.load_dotenv", lambda *a, **k: False)
 29
 30  # ... retry_cfg_fast / sample_job_spec / sample_hits / tmp_csv_dir / settings ...
 31
 32  class FakeMessage:
 33      def __init__(self, value, *, partition=0, offset=0, err=None): ...
 34      def value(self): return self._value
 35      def partition(self): return self._partition
 36      def offset(self): return self._offset
 37      def error(self): return self._err
```

*(Source abridged at lines 30–37 — see the file for the full fixture bodies.)*

**Line-by-line:**
- **Lines 1–5** — docstring states the hermeticity guarantee.
- **Lines 14–22** — import the config dataclasses and `JobSpec` to build fixture values.
- **Lines 25–28** — `_isolate_dotenv` is the single most important fixture: `autouse=True` means it runs
  for **every** test automatically. It monkeypatches `etl.config.load_dotenv` to a no-op so a stray
  developer `.env` on disk can't leak in and break isolation (the lambda swallows any args and returns
  `False`). The `monkeypatch` fixture auto-undoes the patch after each test.
- **Line 30** *(abridged)* — the value fixtures: `retry_cfg_fast` (zero delays so retries don't sleep),
  `sample_job_spec`, `sample_hits`, `tmp_csv_dir` (a temp dir under pytest's `tmp_path`), and
  `settings` (a fully-populated `Settings` with `page_size=2` to force multi-page paths in tests).
- **Lines 32–37** — `FakeMessage`: a stand-in for a `confluent_kafka` message exposing the four methods
  the consumer calls (`value`/`partition`/`offset`/`error`). Exposed to tests via the
  `FakeKafkaMessage` fixture.

## 26. `tests/test_models.py`

```python
 1  from __future__ import annotations
 2
 3  from dataclasses import FrozenInstanceError
 4
 5  import pytest
 6
 7  from etl.errors import RecordCountMismatch
 8  from etl.models import JobSpec
 9
10
11  def test_jobspec_is_immutable() -> None:
12      job = JobSpec("j", "idx", {"match_all": {}}, {}, ["a"], "out.csv")
13      with pytest.raises(FrozenInstanceError):
14          job.data_index = "other"  # frozen dataclass forbids reassignment
15
16
17  def test_record_count_mismatch_carries_context() -> None:
18      err = RecordCountMismatch(expected=5, actual=4, attempts=[(5, 4)])
19      assert err.expected == 5
20      assert err.actual == 4
21      assert "expected=5" in str(err)
```

**Line-by-line:**
- **Line 3** — import `FrozenInstanceError`, the exact exception a frozen dataclass raises on mutation.
- **Lines 11–14** — `test_jobspec_is_immutable`: construct a `JobSpec` positionally (12), then assert
  that reassigning a field raises `FrozenInstanceError` (13–14) — proving `frozen=True` holds.
- **Lines 17–21** — `test_record_count_mismatch_carries_context`: build the error (18) and assert it
  exposes the structured `expected`/`actual` fields (19–20) and includes them in its message (21).

## 27. `tests/test_logging.py`

```python
 1  from __future__ import annotations
 2
 3  import json
 4  import logging
 5
 6  import pytest
 7
 8  from etl.logging_setup import configure_logging
 9
10
11  def test_extra_fields_become_json(capsys: pytest.CaptureFixture[str]) -> None:
12      configure_logging("INFO")
13      logging.getLogger("t").info("hello", extra={"job_id": "abc"})
14      line = capsys.readouterr().out.strip().splitlines()[-1]
15      payload = json.loads(line)
16      assert payload["msg"] == "hello"
17      assert payload["job_id"] == "abc"
```

**Line-by-line:**
- **Line 11** — uses pytest's `capsys` fixture to capture stdout (where the JSON handler writes).
- **Line 12** — configure logging at INFO (installs the `JsonFormatter` on the root logger).
- **Line 13** — log a message with an `extra` dict carrying `job_id`.
- **Line 14** — read captured stdout, take the **last** line (other libraries may log too) — that's the
  record we just emitted.
- **Lines 15–17** — parse it as JSON and assert both the message **and** the merged `extra` field made
  it into the payload — exactly the `logging_setup.py` merge behavior.

## 28. `tests/test_transformer.py`

```python
 1  from __future__ import annotations
 2
 3  import pytest
 4
 5  from etl.errors import TransformError
 6  from etl.transformer import get_by_path, iter_transformed, project
 7
 8
 9  def test_get_by_path_top_level() -> None:
10      assert get_by_path({"a": 1}, "a") == 1
11
12
13  def test_get_by_path_nested() -> None:
14      assert get_by_path({"a": {"b": {"c": 7}}}, "a.b.c") == 7
15
16
17  def test_get_by_path_list_index() -> None:
18      doc = {"users": [{"id": "u1"}, {"id": "u2"}]}
19      assert get_by_path(doc, "users[0].id") == "u1"
20      assert get_by_path(doc, "users[1].id") == "u2"
21
22
23  def test_get_by_path_missing_returns_empty_string() -> None:
24      assert get_by_path({"a": {"b": 1}}, "a.c") == ""
25      assert get_by_path({"a": {"b": 1}}, "missing") == ""
26      assert get_by_path({"a": [1]}, "a[5]") == ""
27
28
29  def test_get_by_path_none_returns_empty_string() -> None:
30      assert get_by_path({"a": None}, "a") == ""
31      assert get_by_path({"a": None}, "a.b") == ""
32
33
34  def test_project_uses_paths_then_falls_back_to_column_name() -> None:
35      hit = {"user": {"id": "u1", "name": "Alice"}, "value": 42}
36      out = project(hit, columns=["id", "name", "value", "missing"],
37                    column_paths={"id": "user.id", "name": "user.name"}, job_id="j")
38      assert out == {"id": "u1", "name": "Alice", "value": 42, "missing": ""}
39
40
41  def test_project_empty_path_raises_transform_error() -> None:
42      with pytest.raises(TransformError):
43          project({"a": 1}, columns=["a"], column_paths={"a": ""}, job_id="j", hit_id="h")
44
45
46  def test_iter_transformed_streams_rows() -> None: ...
47  def test_iter_transformed_with_no_mapping_is_pure_top_level_projection() -> None: ...
```

*(Lines 46–47 abridged; the bodies stream two hits and assert the projected rows, incl. an empty cell
for a missing nested path, and a no-mapping pure top-level projection that drops the unmapped `extra`.)*

**Line-by-line:**
- **Lines 9–14** — `get_by_path` resolves a top-level key and a nested `a.b.c` path.
- **Lines 17–20** — list-index syntax `users[0].id` / `users[1].id`.
- **Lines 23–26** — every "missing" shape (absent nested key, absent top-level key, out-of-range index)
  resolves to `""` — the tolerate-missing-data contract.
- **Lines 29–31** — a present-but-`None` value (and a path *through* a `None`) also yields `""`.
- **Lines 34–38** — `project` uses `column_paths` where given (`id`,`name`), falls back to the column's
  own name for `value`, and emits `""` for a column with no resolvable path (`missing`).
- **Lines 41–43** — a configured **empty** path raises `TransformError` (operator error, fail loud).
- **Lines 46–47** *(abridged)* — `iter_transformed` streams rows over multiple hits, and with an empty
  mapping does a pure top-level projection (dropping fields not in `columns`).

## 29. `tests/test_csv_writer.py`

```python
 1  from __future__ import annotations
 2
 3  import csv
 4  import hashlib
 5  from pathlib import Path
 6
 7  import pytest
 8
 9  from etl.csv_writer import write_csv
10  from etl.errors import CsvWriteError
11
12
13  def test_write_csv_basic(tmp_path: Path) -> None:
14      rows = [{"id": "1", "name": "alpha", "value": 10}, ...]   # 3 rows
15      out = tmp_path / "sub" / "out.csv"
16      res = write_csv(rows, ["id", "name", "value"], out)
17      assert res.row_count == 3
18      assert res.sidecar_path == out.with_suffix(".csv.sha256")
19      # ... round-trip the CSV with csv.reader; header + rows match ...
20      actual = hashlib.sha256(out.read_bytes()).hexdigest()
21      assert res.sha256_hex == actual
22      assert res.sidecar_path.read_text() == f"{actual}  {out.name}\n"
23
24
25  def test_write_csv_handles_missing_fields_and_none(tmp_path) -> None:
26      # rows [{"id":"1"}, {"id":"2","name":None}] -> body ["id,name","1,","2,"]
27
28  def test_write_csv_handles_special_characters(tmp_path) -> None:
29      # value 'a,"b"\nc' is quoted/escaped by csv and round-trips intact
30
31  def test_write_csv_empty_columns_raises(tmp_path) -> None:
32      # write_csv([], [], ...) raises CsvWriteError(match="columns")
33
34  def test_write_csv_zero_rows_writes_header_only(tmp_path) -> None:
35      # write_csv(iter([]), ["a","b"], ...) -> row_count 0, file == "a,b\n"
```

*(Bodies abridged to their essentials — see the file for the full assertions.)*

**Line-by-line / by-test:**
- **`test_write_csv_basic` (13–22)** — the core contract: writes a 3-row CSV (creating the `sub/`
  parent), returns the right `row_count` and sidecar path, the file round-trips through `csv.reader`,
  and crucially the reported `sha256_hex` equals an **independent** recompute of the file's bytes (20–21)
  — proving the one-pass hash matches what `sha256sum -c` would compute. Line 22 checks the exact sidecar
  format.
- **`test_write_csv_handles_missing_fields_and_none` (25–26)** — missing keys and explicit `None` both
  become empty cells (`1,` / `2,`).
- **`test_write_csv_handles_special_characters` (28–29)** — a value containing a comma, quotes, and a
  newline is quoted/escaped by the `csv` module and recovered verbatim on read.
- **`test_write_csv_empty_columns_raises` (31–32)** — empty `columns` → `CsvWriteError`.
- **`test_write_csv_zero_rows_writes_header_only` (34–35)** — passing an empty **iterator** still writes
  the header and reports `row_count == 0` (proving it streams an iterator, not just a list).

## 30. `tests/test_config.py`

```python
 1  from __future__ import annotations
 2  import pytest
 3  from etl.config import load_settings
 4  from etl.errors import ConfigError
 5
 6  _BASE_ENV: dict[str, str] = { "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092", ... }  # all required vars
 7
 8  def _set_env(monkeypatch, env): ...  # clear then set each key
 9
10  def test_load_settings_with_defaults(monkeypatch) -> None:
11      _set_env(monkeypatch, _BASE_ENV)
12      s = load_settings()
13      assert s.es.hosts == ["http://localhost:9200", "http://other:9200"]
14      assert s.pagination.page_size == 1000 and s.pagination.pit_keep_alive == "5m"
15
16  def test_load_settings_missing_required_raises(monkeypatch) -> None:
17      # drop KAFKA_BOOTSTRAP_SERVERS -> ConfigError(match="KAFKA_BOOTSTRAP_SERVERS")
18
19  def test_load_settings_retry_overrides(monkeypatch) -> None:
20      # RETRY_* env vars flow into RetryConfig
21
22  def test_load_settings_bad_integer(monkeypatch) -> None:
23      # PAGE_SIZE="not-a-number" -> ConfigError(match="PAGE_SIZE")
```

*(Source abridged — `_BASE_ENV` lists every required var; `_set_env` clears and re-sets the environment
via `monkeypatch.setenv`/`delenv` so each test is isolated.)*

**By-test:**
- **`test_load_settings_with_defaults`** — with all required vars set, defaults apply: `ES_HOSTS` splits
  on commas into a list, and pagination defaults (1000 / "5m") land.
- **`test_load_settings_missing_required_raises`** — dropping a required var raises `ConfigError`, and
  the message names the missing var (the `match=` regex).
- **`test_load_settings_retry_overrides`** — `RETRY_MAX_ATTEMPTS`/`BACKOFF_*`/`JITTER` env vars are
  parsed into `RetryConfig`.
- **`test_load_settings_bad_integer`** — a non-numeric `PAGE_SIZE` raises `ConfigError` (the
  `_get_int` parse guard). *(Note: this tests parseability, not range — see the adversarial probes for
  the missing `PAGE_SIZE=0`/negative checks.)*

## 31. `tests/test_job_loader.py`

```python
 1  from __future__ import annotations
 2  from unittest.mock import MagicMock
 3  import pytest
 4  from etl.errors import ElasticsearchQueryError, JobSpecError
 5  from etl.job_loader import load_job
 6
 7  def _doc(source: dict) -> dict:
 8      return {"_id": "x", "found": True, "_source": source}
 9
10  def test_load_job_happy_path() -> None:
11      es = MagicMock(); es.get.return_value = _doc({...valid job...})
12      spec = load_job(es, job_index="jobs", job_doc_id="job-7")
13      assert spec.job_id == "job-7" and spec.columns == ["x", "y"]
14
15  # ...missing_doc / missing_fields / bad_columns / es_error_wrapped /
16  #    defaults_empty_column_paths / bad_column_paths...
```

*(Abridged — see the file for the seven full cases.)*

**By-test:**
- **`_doc` helper (7–8)** — wraps a `_source` in the ES `get` envelope (`found: True`).
- **`test_load_job_happy_path`** — a valid doc yields a `JobSpec` with the expected fields.
- **`test_load_job_missing_doc_raises`** — `{"found": False}` → `JobSpecError` (match "not found").
- **`test_load_job_missing_fields_raises`** — a doc missing required fields → `JobSpecError` (match
  "missing").
- **`test_load_job_bad_columns_raises`** — `columns: ["a", 1, "b"]` (a non-string) → `JobSpecError`.
- **`test_load_job_es_error_wrapped`** — `es.get` raising `RuntimeError` is wrapped as
  `ElasticsearchQueryError` (the network-failure path, distinct from malformed-doc).
- **`test_load_job_defaults_empty_column_paths`** — an absent `column_paths` defaults to `{}`.
- **`test_load_job_bad_column_paths_raises`** — `{"a": 5}` (non-string value) → `JobSpecError`.

> The fakes use `unittest.mock.MagicMock`: `es.get.return_value = ...` stubs one call;
> `es.get.side_effect = RuntimeError(...)` makes the call raise.

## 32. `tests/test_extractor.py`

```python
 1  from __future__ import annotations
 2  from unittest.mock import MagicMock
 3  import pytest
 4  from etl.errors import ElasticsearchQueryError
 5  from etl.extractor import expected_count, iter_hits
 6  from etl.models import JobSpec
 7
 8  def _job() -> JobSpec: ...  # job with data_index="d", query={"q": 1}
 9
10  def test_expected_count_returns_int() -> None:
11      es = MagicMock(); es.count.return_value = {"count": 42}
12      assert expected_count(es, "i", {"match_all": {}}) == 42
13      es.count.assert_called_once_with(index="i", body={"query": {"match_all": {}}})
14
15  def test_expected_count_wraps_errors() -> None:
16      # es.count raises -> ElasticsearchQueryError
17
18  def test_iter_hits_streams_source_via_pit() -> None:
19      es.open_point_in_time.return_value = {"id": "pit-1"}
20      es.search.side_effect = [ {page with 2 hits}, {empty page} ]
21      out = list(iter_hits(es, _job(), page_size=10, keep_alive="2m"))
22      assert out == [{"a": 1}, {"a": 2}]
23      es.open_point_in_time.assert_called_once_with(index="d", keep_alive="2m")
24      assert es.search.call_args_list[0].kwargs["body"]["query"] == {"q": 1}
25
26  def test_iter_hits_wraps_errors_in_elasticsearch_query_error() -> None:
27      # open_point_in_time raises -> ElasticsearchQueryError
```

**By-test:**
- **`test_expected_count_returns_int`** — `expected_count` returns the `_count` value **and** calls
  `es.count` with exactly the `_count` body shape (line 13 — verifying the call, not just the result).
- **`test_expected_count_wraps_errors`** — a raising `es.count` surfaces as `ElasticsearchQueryError`.
- **`test_iter_hits_streams_source_via_pit`** — the key integration of the `etl` wrapper over
  `es_extract`: `es.search.side_effect` is a **list**, so consecutive calls return the full page then
  an empty page (terminating the generator). It asserts the streamed `_source` dicts (22), that the
  job's `data_index` and `keep_alive` thread through to `open_point_in_time` (23), and that the job's
  `query` lands in the search body (24).
- **`test_iter_hits_wraps_errors...`** — a failing PIT open surfaces as `ElasticsearchQueryError`.

## 33. `tests/test_es_extract.py`

The standalone package's own tests — note they import **only** from `es_extract`, never `etl`.

```python
 1  """Tests for the standalone `es_extract` package (no `etl` imports)."""
 ...
12  from es_extract import (EsExtractError, SearchAfterPagination, count,
13                          dump_to_ndjson, iter_hits, tee_to_ndjson)
22  def _hit(src, sort=None):
23      h = {"_source": src, "_id": src.get("id")}; ...   # builds a realistic hit envelope
```

```python
57  def test_search_after_pages_and_closes_pit() -> None:
58      es = MagicMock(); es.open_point_in_time.return_value = {"id": "pit-1"}
60      es.search.side_effect = [ {2 hits}, {1 hit}, {empty} ]   # three pages
66      out = list(SearchAfterPagination().iter_hits(es=es, index="i", query={}, page_size=2))
67      assert out == [{"id": 1}, {"id": 2}, {"id": 3}]
69      bodies = [c.kwargs["body"] for c in es.search.call_args_list]
70      assert "search_after" not in bodies[0]
71      assert bodies[1]["search_after"] == [2]   # cursor = prev page's last sort
72      assert bodies[2]["search_after"] == [3]
74      es.close_point_in_time.assert_called_once_with(body={"id": "pit-1"})
```

**By-test (the 11 cases):**
- **`_hit` helper (22–26)** — builds a hit with `_source`, `_id`, and optional `sort`, mirroring a real
  ES response so the tests exercise the envelope handling.
- **`test_count_returns_int` / `_wraps_errors_with_default` / `_with_injected_error_cls`** — `count`
  returns the value and calls the right body; a failure wraps in `EsExtractError` by default, or in an
  **injected** `error_cls` when supplied (proving the DI seam).
- **`test_search_after_pages_and_closes_pit`** — the core: three pages via `side_effect`; asserts all
  hits stream in order (67), the **cursor threading** (the first body has no `search_after`, each
  subsequent body carries the previous page's last `sort`, lines 70–72), and that the PIT is **closed
  exactly once** (74).
- **`test_search_after_source_only_false_yields_full_envelope`** — with `source_only=False`, the yielded
  value is the whole hit (incl. `_id`) — the opt-in escape hatch.
- **`test_search_after_closes_pit_on_early_close`** — pull one hit then `gen.close()`; the PIT is still
  closed (the `finally` runs on early abandonment) — the make-or-break lifecycle property.
- **`test_search_after_open_error_does_not_close`** — if `open_point_in_time` raises, `close` is **not**
  called (nothing was opened) and the error wraps.
- **`test_search_after_wraps_errors_with_injected_cls`** — a failing `search` wraps in the injected
  class **and** still closes the PIT (cleanup runs on the error path).
- **`test_iter_hits_convenience_streams_via_pit`** — the one-call `iter_hits` wires through to a PIT.
- **`test_tee_to_ndjson_passes_through_and_writes` / `test_dump_to_ndjson_returns_count`** — the tee
  yields hits unchanged while writing valid NDJSON (creating parent dirs); the eager dump returns the
  count and writes one line per hit.

## 34. `tests/test_retry.py`

```python
 8  from etl.retry import retry, retry_call
11  class Boom(Exception): pass        # the test's "retryable" exception
15  def test_retry_succeeds_first_try_no_sleep() -> None:
17      result = retry_call(lambda: 42, on=(Boom,), attempts=5, sleeper=sleeps.append)
23      assert result == 42 and sleeps == []
27  def test_retry_succeeds_on_third_attempt_sleeps_twice() -> None:
31      def flaky(): ...               # raises Boom twice, then returns "ok"
44          sleeper=sleeps.append)
47      assert sleeps == [1.0, 2.0]    # 1*2^0, 1*2^1, jitter disabled
```

**By-test (the 8 cases) — note the test technique: `sleeper=sleeps.append` records the delays instead
of sleeping, so backoff is asserted exactly and instantly:**
- **`test_retry_succeeds_first_try_no_sleep`** — success on the first call returns immediately with
  **zero** sleeps.
- **`test_retry_succeeds_on_third_attempt_sleeps_twice`** — a function that fails twice then succeeds
  produces exactly two sleeps of `[1.0, 2.0]` — verifying `base * 2**attempt` with jitter off.
- **`test_retry_exhausts_and_raises_original`** — 3 always-failing attempts → 2 sleeps, then the
  **original** `Boom` is re-raised (default behavior).
- **`test_retry_wrap_final_emits_retry_exhausted`** — with `wrap_final=True`, the final raise is
  `RetryExhausted` carrying `attempts` and the original `last_exc`.
- **`test_retry_does_not_catch_unrelated_exceptions`** — an exception **not** in `on=` propagates
  immediately with no retries/sleeps.
- **`test_retry_decorator_form`** — the `@retry(...)` decorator behaves identically.
- **`test_retry_attempts_must_be_positive`** — `attempts=0` raises `ValueError` (the guard).
- **`test_retry_jitter_uses_injected_rng`** — with a seeded `random.Random(0)` and `jitter=0.5`, the
  single delay lands within the expected `[0.5, 1.5]` band — proving jitter is applied and the RNG is
  injectable (deterministic).

## 35. `tests/test_validator.py`

```python
14  def _csv_result(rows: int) -> CsvResult: ...   # a CsvResult with the given row_count
23  def test_validate_counts_equal_ok() -> None:
24      validate_counts(5, 5)                       # no raise
27  def test_validate_counts_unequal_raises() -> None:
28      with pytest.raises(RecordCountMismatch): validate_counts(5, 4)
75  def test_validate_with_retry_triggers_reextract_and_succeeds(retry_cfg_fast) -> None:
81      es.count.side_effect = [{"count": 99}] * 5 + [{"count": 7}]   # 5 mismatch, then match
82      reextract = MagicMock(return_value=_csv_result(7))
84      res = validate_with_retry(es=es, ..., on_full_reextract=reextract, sleeper=lambda _: None)
91      assert res.row_count == 7
92      reextract.assert_called_once() and es.count.call_count == 6
```

**By-test (the 6 cases) — the technique is `es.count.side_effect = [...]` to script a *sequence* of
`_count` results across retries:**
- **`test_validate_counts_equal_ok` / `_unequal_raises`** — the one-shot `validate_counts`: equal is a
  no-op, unequal raises `RecordCountMismatch`.
- **`test_validate_with_retry_first_count_matches_no_reextract`** — when the very first `_count` matches,
  it returns after **one** call and the re-extract callback (a `MagicMock` that would assert if invoked)
  is **never** called.
- **`test_validate_with_retry_recovers_after_a_couple_flaps`** — two mismatches then a match → returns on
  the 3rd `_count`, still **no** re-extract (Tier 1 recovery — the refresh-race scenario).
- **`test_validate_with_retry_triggers_reextract_and_succeeds`** — all 5 Tier-1 attempts mismatch, the
  re-extract runs (returning a corrected 7-row result), and the 6th `_count` matches it → success.
  Asserts `reextract` called once and exactly 6 `_count` calls.
- **`test_validate_with_retry_final_mismatch_raises`** — 5 mismatches + re-extract + a final mismatch →
  `RecordCountMismatch` whose `attempts` history has **6** entries (5 Tier-1 + 1 Tier-2), with the right
  `expected`/`actual`.

## 36. `tests/test_control_consumer.py`

The most "fake-heavy" test — it supplies a hand-written `FakeConsumer` via the injectable factory.

```python
10  class FakeConsumer:
11      def __init__(self, cfg): self.cfg = cfg; self.subscribed = []; self.committed = []
15          self.queue = []                 # messages to hand out from poll()
21      def poll(self, timeout): return self.queue.pop(0) if self.queue else None
26      def commit(self, *, message, asynchronous): self.committed.append(message)
29      def close(self): self.closed = True
42  def test_consumer_subscribes_with_manual_commit_config() -> None:
50      cc = ControlConsumer(_cfg(), consumer_factory=factory)
51      assert fake_holder[0].subscribed == ["ctl"]
52      assert fake_holder[0].cfg["enable.auto.commit"] == "false"
```

**By-test (the 4 cases):**
- **`FakeConsumer` (10–30)** — implements just what `ControlConsumer` uses: `subscribe`, `poll` (pops
  from a scripted `queue`, returns `None` when empty), `commit` (records the committed message), and
  `close`. This is dependency injection paying off — no `confluent_kafka` needed.
- **`test_consumer_subscribes_with_manual_commit_config`** — constructing the consumer subscribes to the
  control topic and the passed config has `enable.auto.commit=false` (manual commit). `close()` closes
  the fake.
- **`test_consumer_decodes_valid_message_and_commit_only_after_ack`** — a valid JSON message is decoded
  into a `ControlMessage` with the right `job_doc_id`/partition/offset; crucially `fake.committed == []`
  **before** calling `commit()` and `== [raw]` **after** — proving the commit is caller-controlled, not
  automatic. Uses `stop=` to end the generator and `it.close()` to finalize it.
- **`test_consumer_skips_null_value_message`** — a null-valued (tombstone) record is committed past, and
  the next good message is the one yielded (one prior commit recorded).
- **`test_consumer_skips_poison_message_by_committing`** — `b"not-json"` is poison: it's committed past
  (1 commit) so the loop doesn't stick, then the good message is yielded and acked (2 commits).

## 37. `tests/test_pipeline.py`

The end-to-end test of `run_one` with a fully faked `es` and a monkeypatched `subprocess.run`.

```python
23  def _es_with_job_and_hits(hits) -> MagicMock:
25      es.get.return_value = {"found": True, "_source": {...job doc...}}
36      es.count.return_value = {"count": len(hits)}
37      es.open_point_in_time.return_value = {"id": "pit-1"}
43      page  = {"pit_id": "pit-1", "hits": {"hits": [{"_source": h, "sort": [i]} ...]}}
47      empty = {"pit_id": "pit-1", "hits": {"hits": []}}
49      def _search(**kwargs):                     # keyed on the body, so re-extract works too
50          return empty if "search_after" in kwargs["body"] else page
52      es.search.side_effect = _search
56  def test_pipeline_golden_path(monkeypatch, settings, tmp_path) -> None:
60      es = _es_with_job_and_hits([{"id":"1","name":"Alice"}, {"id":"2","name":"Bob"}])
61      monkeypatch.setattr(subprocess, "run", lambda *a, **kw: MagicMock(returncode=0, stderr=b""))
64      run_one(ctrl=_ctrl(), es=es, settings=settings)
68      assert staged.exists() and sidecar.exists()
71      assert len(staged.read_text().splitlines()) == 3      # header + 2 rows
```

**By-test (the 5 cases):**
- **`_es_with_job_and_hits` helper (23–53)** — builds one `MagicMock` ES that answers `get` (the job
  doc), `count`, `open_point_in_time`, and `search`. The clever bit (49–52): `search.side_effect` is a
  **function** keyed on whether the body has `search_after` — returns the full page first, the empty
  page after — so it terminates correctly **and** works for the re-extract path (which opens a fresh PIT
  and pages again).
- **`test_pipeline_golden_path`** — the happy path: faked ES + a `subprocess.run` stub returning
  rc 0; `run_one` produces the staged CSV + sidecar with header + 2 rows. (This is the unit-level mirror
  of the live smoke test.)
- **`test_pipeline_count_mismatch_after_reextract_raises`** — `es.count` always returns 99 vs 1 row →
  `RecordCountMismatch`; SFTP is never invoked (failure halts before delivery).
- **`test_pipeline_sftp_failure_after_retries`** — `subprocess.run` always returns rc 1 → `SftpUploadError`
  after the retries.
- **`test_pipeline_transient_sftp_recovers`** — `run` fails twice then succeeds (a counter closure) →
  `run_one` completes; asserts exactly 3 attempts.
- **`test_pipeline_raw_dump_writes_ndjson`** — uses `dataclasses.replace(settings, raw_dump_dir=...)` to
  enable the diagnostic, then asserts `<dir>/job-1.ndjson` contains the **raw `_source` hits**, one JSON
  object per line.

## 38. `tests/test_sftp_uploader.py`

```python
12  from etl.sftp_uploader import UploadPlan, _build_batch, upload
26  def test_build_batch_quotes_paths() -> None:
31      text = _build_batch([UploadPlan(Path("/local/a b.csv"), "/remote/a b.csv"), ...])
32      assert "'/local/a b.csv'" in text and text.strip().endswith("bye")
37  def test_upload_invokes_sftp_with_strict_host_checking(monkeypatch, tmp_path, retry_cfg_fast):
42      def fake_run(argv, **kwargs): captured["argv"] = argv; return MagicMock(returncode=0, stderr=b"")
46      monkeypatch.setattr(subprocess, "run", fake_run)
48      upload(cfg, [UploadPlan(...)], retry_cfg=retry_cfg_fast, sleeper=lambda _: None)
55-63  assert "StrictHostKeyChecking=yes" in argv and "BatchMode=yes" in argv and ...
```

**By-test (the 6 cases) — `subprocess.run` is always monkeypatched, so no real `sftp` runs:**
- **`test_build_batch_quotes_paths`** — paths with spaces are `shlex.quote`d in the batch text, which
  ends with `bye`.
- **`test_upload_invokes_sftp_with_strict_host_checking`** — captures the `argv` and asserts the whole
  hardened command: `sftp -b -i <key> -P 2222 -o UserKnownHostsFile=... -o StrictHostKeyChecking=yes -o
  BatchMode=yes user@host`. This is the security contract pinned as a test.
- **`test_upload_retries_then_succeeds`** — fails twice then rc 0 → 3 attempts total.
- **`test_upload_raises_after_exhausting_retries`** — always rc 1 → `SftpUploadError` (matching the
  stderr text) after `max_attempts` tries.
- **`test_upload_timeout_raises_sftp_error`** — `run` raising `subprocess.TimeoutExpired` → `SftpUploadError`
  (match "timed out").
- **`test_upload_missing_sftp_binary_raises_sftp_error`** — `run` raising `FileNotFoundError` →
  `SftpUploadError` (match "not found").

## 39. `tests/test_adversarial.py`

The probes added in this session: each asserts the *desirable* behavior, so **9 fail by design** to
document real gaps; 3 pass. (Full source is in the repo; this is the intent per probe.)

```python
# Each test asserts what a careful user expects; a FAILURE = a real defect/gap.
def test_hashing_writer_returns_bytes_written():       # ❌ returns char count (§3.4)
def test_csv_formula_injection_is_neutralized():       # ❌ '=1+1' written raw (security)
def test_nested_value_serializes_as_json():            # ❌ Python repr, not JSON (§3.5)
def test_csv_unicode_hash_round_trips():               # ✅ robust
def test_zero_page_size_rejected():                    # ❌ PAGE_SIZE=0 accepted
def test_negative_page_size_rejected():                # ❌ PAGE_SIZE=-5 accepted
def test_empty_columns_rejected():                     # ❌ columns:[] accepted at load
def test_non_dict_query_rejected():                    # ✅ robust
def test_dot_only_path_does_not_leak_whole_document(): # ❌ "." returns the whole doc
def test_document_id_is_projectable():                 # ❌ _id unreachable (§3.1)
def test_huge_list_index_is_safe():                    # ✅ robust
def test_compute_delay_does_not_overflow_at_high_attempt():  # ❌ OverflowError
```

**By-probe (the 12):** see `REVIEW.v3.md` §D.3 for the consolidated table and recommended fix order.
The 9 failures map to the inline notes in Parts I–II: `_HashingWriter.write` return (#1, §12),
formula injection (#2, §12), nested-`repr` (#3, §12), `PAGE_SIZE` range (#4/#5, §10), empty `columns`
(#6, §16), `"."`-path leak (#7, §11), `_id` projection (#8, §11/§15), and `_compute_delay` overflow
(#9, §13). The 3 passes (unicode hash, non-dict query rejected, huge index safe) confirm those paths
are robust.

> These tests are intentionally red — they're documentation-as-tests. `REVIEW.v3.md` §D.3 lists the
> options (fix the 9 defects, or mark them `xfail` with reasons) to restore a green bar.

---

# Part V — operational scripts (`scripts/`)

These are not part of the shipped service — they seed, prototype, and stand up the local stack. Covered
here by section + key lines (full source in the repo).

## 40. `scripts/seed.py`

Seeds the local stack: a job doc, 5 sample data docs, and one control message.

```python
21  from confluent_kafka import Producer
22  from elasticsearch import Elasticsearch
24  ES_URL = "http://localhost:9200"; KAFKA_BROKERS = "localhost:9092"
26  JOB_INDEX = "etl-jobs"; DATA_INDEX = "sales-2026-05"; CONTROL_TOPIC = "etl.control"
31  JOB_DOC = { "job_id": ..., "data_index": DATA_INDEX, "query": {"match_all": {}},
35            "column_paths": {"order_id":"order_id","customer":"customer.name",
37                             "amount":"totals.amount_cents","first_sku":"items[0].sku"},
41            "columns": [...], "remote_filename": "upload/daily-sales-2026-05-26.csv" }
48  def _sample_doc(i): return {"order_id": f"o-{i:03d}", "customer": {"name": ...}, ...}
67  def main() -> int:
68      es = Elasticsearch(ES_URL); _wait_for_es(es)
72      if not es.indices.exists(index=JOB_INDEX): es.indices.create(index=JOB_INDEX)
74      es.index(index=JOB_INDEX, id=JOB_DOC_ID, document=JOB_DOC, refresh="wait_for")
78      if es.indices.exists(index=DATA_INDEX): es.indices.delete(index=DATA_INDEX)   # reset
80      es.indices.create(index=DATA_INDEX)
81      for i in range(5): es.index(index=DATA_INDEX, document=_sample_doc(i))
83      es.indices.refresh(index=DATA_INDEX)
88      producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
89      payload = json.dumps({"job_doc_id": JOB_DOC_ID, "correlation_id": f"smoke-{...}"}).encode()
92      producer.produce(CONTROL_TOPIC, payload); producer.flush(10)
```

**Key lines:**
- **Lines 21–22** — imports the **real** Kafka producer and ES client (this script *does* need infra,
  unlike the tests).
- **Lines 31–45** — `JOB_DOC`: the exact job document shape the loader validates; its `column_paths`
  demonstrate all four path kinds (top-level, nested ×2, list index). `remote_filename` targets
  `upload/` (the SFTP user's writable dir).
- **Lines 48–54** — `_sample_doc(i)`: a deterministic order document; 5 of these are indexed.
- **Line 57 (`_wait_for_es`)** — polls cluster health (yellow) so the script doesn't race ES startup.
- **Line 74** — indexes the job doc with `refresh="wait_for"` so it's immediately searchable.
- **Lines 78–83** — **resets** the data index (delete-then-create) for a deterministic count, indexes 5
  docs, and refreshes so they're visible.
- **Lines 88–92** — produces one control message (`job_doc_id` + a `correlation_id`) and `flush`es so it
  actually lands before the script exits.

## 41. `scripts/prototype.py`

Hand-drives the core (offline) or the live extract→transform→CSV chain (`--live`).

```python
40  sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))  # run without pip install
51  SAMPLE_HITS = [ {full order}, {full order}, {ragged: no customer, empty items} ]
61  COLUMNS = [...]; COLUMN_PATHS = {...}      # the spec under test — edit and re-run
74  def transform_to_csv(hits, columns, column_paths, out_path, *, job_id) -> CsvResult:
84      rows = iter_transformed(hits, column_paths, columns, job_id=job_id)
85      return write_csv(rows, columns, out_path)         # the exact chain run_one uses
88  def _report(result, expected) -> int:                 # print CSV, verify sha256, validate count
115 def run_offline(out) -> int: ...                      # get_by_path spot-checks + transform_to_csv
135 def run_live(args, out) -> int:
138     from elasticsearch import Elasticsearch          # imported lazily — offline needs no ES
145     job = load_job(es, job_index=..., job_doc_id=...)
157     expected = expected_count(es, job.data_index, job.query)
160     hits = list(iter_hits(es, job, page_size=..., keep_alive=...))
163     result = transform_to_csv(hits, job.columns, job.column_paths, out, job_id=job.job_id)
```

**Key lines:**
- **Line 40** — prepends `src/` to `sys.path` so `import etl...` works even without `pip install -e .`
  (a self-contained sandbox).
- **Lines 51–58** — `SAMPLE_HITS` includes a deliberately **ragged** record (no `customer`, empty
  `items`) so offline mode demonstrates missing paths → empty cells rather than crashes.
- **Lines 61–67** — the editable `COLUMNS`/`COLUMN_PATHS` — the whole point of offline mode is to tweak
  these and *see* the resulting CSV.
- **Lines 74–85** — `transform_to_csv`: the reusable seam — the **exact** `iter_transformed → write_csv`
  chain `pipeline.run_one` uses, so what you see here is what the daemon does.
- **Lines 88–112** — `_report`: prints the CSV, recomputes the SHA256 and compares to the sidecar, and
  runs `validate_counts` — a mini version of the daemon's integrity checks.
- **Lines 115–132** — `run_offline`: prints `get_by_path` spot-checks (including the ragged doc) then
  builds and reports the CSV from `SAMPLE_HITS`.
- **Lines 135–164** — `run_live`: lazily imports the ES client (138, so offline never needs it), loads
  the job, runs `expected_count` + `iter_hits` (PIT + search_after) against the seeded ES, and reports.
- **Lines 167–189** — `main`: argparse with `--live`, output path, ES/job knobs, and `--verbose` (turns
  on the structured logs); dispatches to live or offline.

## 42. `scripts/try_es_extract.py`

Exercises the standalone `es_extract` against a real ES — proving its `etl`-independence.

```python
43  sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
45  from es_extract import count, iter_hits, tee_to_ndjson
49  try:
50      from etl.models import JobSpec                    # prefer the real dataclass…
51  except Exception:                                     # …but fall back if etl isn't present
52      from dataclasses import dataclass, field
54      @dataclass(frozen=True)
55      class JobSpec: ...                                # minimal stand-in (data_index + query)
75  def _seed(es, index, n): ...                          # (re)create index with n sample docs
152 total = count(es, job.data_index, job.query)
156 hits = iter_hits(es, job.data_index, job.query, page_size=..., source_only=not args.full_envelope)
161 if dump_path is not None: hits = tee_to_ndjson(hits, dump_path)
180 ok = streamed == total                                # cross-check stream vs _count
```

**Key lines:**
- **Lines 45** — imports **only** from `es_extract` — the harness deliberately avoids needing `etl`.
- **Lines 49–61** — the `try/except` import is the proof of decoupling: it *prefers* `etl.models.JobSpec`
  but falls back to a minimal local dataclass, so the script runs even if `es_extract/` was copied out
  on its own. (`# pragma: no cover` — only hit outside this repo.)
- **Lines 64–72 (`_connect`)** — builds an `Elasticsearch` client with optional api-key/basic-auth.
- **Lines 75–90 (`_seed`)** — optionally (re)creates a throwaway index with N sample docs and refreshes;
  uses `es.options(ignore_status=[400,404])` so delete-if-exists is idempotent.
- **Lines 93–119** — argparse: `--seed`/`--cleanup`, `--index`/`--query`, `--page-size`/`--keep-alive`,
  `--full-envelope` (toggles `source_only`), `--dump` (NDJSON), auth flags.
- **Lines 152–162** — the actual exercise: `count`, then `iter_hits` (optionally `tee_to_ndjson`'d),
  with `source_only` driven by `--full-envelope`.
- **Lines 164–183** — stream and print up to `--limit` hits, optionally clean up the seeded index, and
  **cross-check** that the streamed count equals `_count` (the script's pass/fail signal, exit 0/1).

## 43. `scripts/setup_local.sh`

Brings up the stack and prepares keys/topic. (Bash; covered in `REVIEW.v3.md` §A.2.)

```bash
 6  set -euo pipefail                                # strict mode: fail on error/unset/pipe-fail
16  if [ ! -f "$KEY_DIR/id_ed25519" ]; then ssh-keygen -t ed25519 ...; fi    # ETL client key
24  for kt in rsa ed25519; do ... ssh-keygen ... ; done                     # persistent SFTP host keys
35  docker compose -f "$ROOT/docker-compose.yml" up -d
39  for _ in $(seq 1 60); do curl -sf .../_cluster/health && break; sleep 1; done   # wait for ES
69  ssh-keyscan -p 2222 -H -t ed25519,rsa localhost > "$KEY_DIR/known_hosts"        # capture host key
74  docker exec etl-kafka kafka-topics ... --create --if-not-exists --topic etl.control
```

**Key lines:**
- **Line 6** — `set -euo pipefail`: abort on any error, unset variable, or failed pipe component — the
  standard safe-bash preamble.
- **Lines 16–20** — generate the ETL client `ed25519` keypair if absent, and install the public key as
  the SFTP `authorized_keys` (idempotent: only if missing).
- **Lines 24–31** — generate **persistent** SFTP host keys (bind-mounted into the container) so
  `known_hosts` stays valid across restarts.
- **Line 35** — `docker compose up -d`.
- **Lines 38–45** — poll ES health (≤60 tries). *(REVIEW §A.2 note: ES's cold first boot can exceed
  this; the script is idempotent, so re-running finishes the remaining steps.)*
- **Lines 47–65** — wait for SFTP (port 2222) and Kafka (broker API) similarly.
- **Lines 67–70** — `ssh-keyscan` captures the SFTP host key into `known_hosts` — this is what makes
  `StrictHostKeyChecking=yes` work locally.
- **Lines 72–78** — explicitly create the `etl.control` topic (`--if-not-exists`), even though
  auto-create is on.
- **Lines 80–93** — print the connection summary and next steps.

## 44. `scripts/teardown_local.sh`

```bash
 4  set -euo pipefail
 8  docker compose -f "$ROOT/docker-compose.yml" --profile ui down -v
10  if [ "${1:-}" = "--purge" ]; then rm -rf "$ROOT/local/keys" "$ROOT/local/sftp"; fi
```

**Key lines:**
- **Line 4** — strict-mode preamble.
- **Line 8** — `down -v` stops and removes containers **and** volumes; `--profile ui` ensures the opt-in
  Kibana/kafka-ui containers are torn down too (even if started separately). Because ES has no named
  volume, this gives a clean slate next boot.
- **Line 10** — `--purge` additionally wipes the generated `local/keys` and `local/sftp` (keys, host
  keys, and the upload drop dir). Without it, delivered CSVs persist — the stale-file foot-gun noted in
  `REVIEW.v3.md` §A.6.

---

*Every file under `src/`, `tests/`, `scripts/`, and the root packaging/config is now walked above.
The only remaining files are `tests/__init__.py` (empty) and the stack/runtime files documented in
`REVIEW.v3.md` §A.*






# What's not here (and how to get it)

This v3 now walks every line of the **source** (`src/etl/` + `src/es_extract/`), the **packaging/config**
files (Part III), the **tests** (Part IV), and the **operational scripts** (Part V). What remains:

- **`tests/__init__.py`** — an empty package marker (0 lines); it exists only so `tests/` is a package.
- **`docker-compose.yml`, `.env.local`, the `local/` runtime tree** — documented in
  [`REVIEW.v3.md`](REVIEW.v3.md) §A (Bootstrap & Live Testing), including the verified end-to-end smoke
  run.

# Cross-references

- **Why** each decision was made: [`DESIGN.md`](DESIGN.md) (§ numbers cited throughout).
- **What to build in what order** (the ticketed build log): [`TUTORIAL.md`](TUTORIAL.md) /
  [`TUTORIAL.v2.md`](TUTORIAL.v2.md).
- **Findings, design-decision index, and full file coverage**: [`REVIEW.v3.md`](REVIEW.v3.md).
- **Open items surfaced inline above** (adversarial #1/#3/#6/#7/#8/#9, REVIEW §3.2/§3.3): see
  [`REVIEW.v3.md`](REVIEW.v3.md) §D for the consolidated list and recommended fix order.






