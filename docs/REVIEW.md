# Documentation & Code Review — Errata and Improvements

**Subject:** `docs/TUTORIAL.md` and `docs/DESIGN.md` (with the code and `README.md` they describe).
**Goal of this document:** make the two long-form docs *correct* and *complete* against the code that
actually ships, and record the code-level blind spots they surface. It is a companion to — and a
punch-list for — `DESIGN.md` (the theory) and `TUTORIAL.md` (the build log).

> **⚠️ Superseded in part by the PIT-only refactor (2026-06).** After this review, the **Scroll
> pagination strategy was removed entirely** and the `src/etl/pagination/` package was deleted:
> extraction is now **point-in-time + `search_after` only**, living in the standalone `es_extract`
> package with `etl/extractor.py` as the single thin wrapper. `DESIGN.md` and `TUTORIAL.md` were
> rewritten to match (Scroll now appears only as a one-line "legacy NiFi mechanism we chose against").
> The **Scroll-specific findings below are therefore moot** — T1, T2, and the `scroll.py` references in
> the verified-facts list and §3.1/T5. Post-refactor state: **72 tests, `ruff` + `mypy --strict`
> clean, ~88% coverage.**

**Method.** Every source module under `src/etl/` was read, both docs were read end-to-end, and the
claims were checked against a live run of the suite. Where a finding is empirical, the evidence is
shown inline. Priority, per the review brief, is **TUTORIAL.md correctness and whether it covers all
of the code** — that is Part 1.

**Verified facts used throughout (so later sections can reference them):**

> **Update:** many of the findings below have since been applied — see the **Resolution status**
> section right after the legend. The figures in this section are *as found at review time*; the
> post-fix state is **72 tests / ~88% coverage** (after the later PIT-only refactor — it was 82
> before Scroll's tests were removed; see the Resolution status section).

- `pytest` collected **64 tests** at review time; they passed with **83%** line coverage
  (`pytest --cov=etl`). *(Now **67 / ~86%** after the two added test files.)*
- `__main__.py` and `logging_setup.py` had **0% unit coverage** — exercised only by the manual smoke
  test. *(`logging_setup.py` is now ~96% after `tests/test_logging.py` was added; `__main__.py`
  remains 0% by design.)*
- The pagination strategies yield **`_source` only** (`scroll.py:47`, `search_after.py:57`:
  `yield h.get("_source", {})`). Everything downstream — including the document `_id` — flows from
  that single fact, and it drives the most important finding below (§3.1).

---

## Severity legend

| Tag | Meaning |
|-----|---------|
| **P0** | Wrong/broken as written — a reader following it hits an error, or it documents behavior the code does not have. Fix first. |
| **P1** | Misleading or materially incomplete — not strictly false, but it will cause a wrong mental model or a missing capability. |
| **P2** | Polish — staleness, nits, small omissions. |

---

## Resolution status

The findings below were reviewed with the repo owner; this section records what was **applied** and
what was deliberately **deferred**. The rest of the document is kept as the as-found catalogue and
rationale. The doc-review fixes brought the suite to 67 tests / ~86%; the **follow-on work** below
took it to **82 tests, `ruff` + `mypy --strict` clean, ~88% coverage**.

**Applied**

| Finding | What changed |
|---|---|
| T1 / T2 | TUTORIAL ETL-C1 `scroll.py` now shows the real `try/except → ElasticsearchQueryError` + `scroll_id is None` guard (lints clean; matches repo). |
| T3 | TUTORIAL ETL-E2 inlines the real `keep_alive` ternary instead of the non-existent `_keep_alive(settings)`. |
| T4 | Added `tests/test_models.py` and `tests/test_logging.py` (the A2/A4 tests); `logging_setup.py` coverage 0% → ~96%. |
| A2/A4 snippets | TUTORIAL A2 test uses `FrozenInstanceError` (was `pytest.raises(Exception)` → ruff B017); A4 splits `import json, logging` (was ruff E401). Both snippets now pass the tutorial's own gate. |
| job_loader / consumer reconcile | TUTORIAL ETL-C2 now includes the `found` + non-empty `remote_filename` checks; ETL-E1 `_decode` now includes the `correlation_id` type check — matching the shipped modules. |
| D1 / D2 / D3 | DESIGN.md header restored (`# Design & Implementation Guide`), stray `33` removed, `safeHiHHHty` → `safety`. |
| R1 | README job-doc example maps `order_id` → `order_id` (was the broken `_id`). **Example-only fix** per decision (see Deferred). |
| R2 | README "63 tests" → "67"; DESIGN "64 / ~83%" → "67 / ~86%". |
| R3 / §3.2 (metadata) | `pyproject.toml` description: `… -> JOLT -> …` → `… -> dotted-path projection -> …`. |

**Deferred (intentional)**

| Finding | Why deferred |
|---|---|
| §3.1 / T5 — `_id` capability gap | Decision: **fix the README example only.** The `_source`-only limitation is *not* documented and the code is unchanged, so the latent `hit.get("_id")` no-op and the inability to project `_id` remain — just no longer advertised by a broken example. Revisit if a job ever needs `_id` as a column. |
| §3.2 (code) — `TransformError.jolt_op` + JOLT docstring | Code left as-is; the TUTORIAL A2 snippet still mirrors it, so doc and code stay aligned. Pure cleanup, no behavior. |
| §3.3 — SFTP destination atomicity | Real hardening (temp-then-rename / documented sidecar-ordering contract), but a behavior change — left for a ticket. |
| §3.4 / §3.5 / §3.6 | `_HashingWriter` return value, nested-value stringification, redundant initial `_count` — harmless; left as noted. |
| §2.3 (C1–C3), N1, N2 | DESIGN completeness paragraphs and tutorial cross-links — additive polish, not applied. |

**Follow-on work (post-review, by request)**

| Change | Detail |
|---|---|
| Standalone `es_extract` package (later made PIT-only) | The ES extraction layer (`count`, one-call `iter_hits`, the pagination generator) moved into `src/es_extract/` — deps: `elasticsearch` + stdlib only, **zero** `etl` imports — so it is reusable in other projects. A subsequent refactor **removed Scroll** and deleted `etl/pagination/`; `etl/extractor.py` is now the single thin wrapper injecting `ElasticsearchQueryError` via an `error_cls` parameter, preserving the `EtlError` boundary. Documented in DESIGN §16–20 + appendix, README, and (this time) **TUTORIAL.md ETL-C1**, which was rewritten PIT-only. A standalone harness, `scripts/try_es_extract.py`, exercises the package against a real ES. |
| Streaming NDJSON diagnostic | `es_extract.diagnostics.tee_to_ndjson` captures the raw hit stream to disk without buffering. Wired into the pipeline opt-in via the new `ES_RAW_DUMP_DIR` env var (unset = disabled): each job dumps `<dir>/<job_id>.ndjson`. New tests cover the package (`tests/test_es_extract.py`) and the pipeline wiring. |
| Note on §3.1 (`_id`) | The new `es_extract` strategies accept `source_only=False` to yield the **full hit envelope** (incl. `_id`). The `_id` capability gap now has a clean, opt-in path at the reusable layer if a future job needs it — though `etl`'s wrappers still default to `_source` (unchanged behavior). |

---

# Part 1 — TUTORIAL.md (priority): correctness & coverage

The tutorial is genuinely good: the phase/ticket structure, the "never start a ticket until the last
milestone is green" rule, and the Python/Kafka/ES deep-dives are excellent. The issues below are
narrow but real, and a few of them break the tutorial's own central promise — *every ticket ends on a
green `ruff`/`mypy`/`pytest`*.

## 1.1 Correctness defects

### T1 (P0) — the printed `scroll.py` fails the tutorial's own `ruff` gate

ETL-C1 prints `src/etl/pagination/scroll.py` (TUTORIAL.md ~line 867) as if it were the complete file,
but the printed body **removes the `try/except` wrappers** that the real
[`scroll.py`](../src/etl/pagination/scroll.py) has, while **keeping the import**:

```python
from etl.errors import ElasticsearchQueryError   # imported…
# …but the printed body never raises it (no try/except around es.search/es.scroll)
```

Phase A wires `ruff` with the `F` rule family selected. Linting the block verbatim:

```
F401 [*] `etl.errors.ElasticsearchQueryError` imported but unused
 --> scroll.py:8:24
```

So a reader who types ETL-C1 along gets a **red `ruff check`**, directly contradicting ETL-C1's
milestone ("both pagination tests green … mypy clean") and the golden rule. **Fix:** print the real
file (restore the two `try/except … raise ElasticsearchQueryError(...)` blocks and the
`if scroll_id is None: break` guard), or drop the unused import from the teaching snippet.

### T2 (P1) — `scroll.py` is silently simplified; `search_after.py` is flagged but `scroll.py` is not

The repo's `scroll.py` wraps both ES calls in `try/except → ElasticsearchQueryError` and guards
`if scroll_id is None: break`. The tutorial's version has none of that, yet presents it as the file
to write (no "see repo for the full version" note). By contrast, ETL-C1 *does* flag `search_after.py`
as a fragment ("same shape … see repo for the full file"). Treat both the same: either show the real
error-handling, or label `scroll.py` as simplified the way `search_after.py` already is. This matters
because robust ES error handling is one of the behaviors the design sells (DESIGN §18, "Errors from
the ES calls are wrapped in `ElasticsearchQueryError`") — the tutorial quietly drops it.

### T3 (P0) — ETL-E2's `pipeline.py` calls a helper that does not exist

The ETL-E2 snippet (TUTORIAL.md ~line 1493/1499) calls `_keep_alive(settings)`:

```python
strategy = make_strategy(settings.pagination.strategy, keep_alive=_keep_alive(settings))
```

There is no `_keep_alive` in [`pipeline.py`](../src/etl/pipeline.py) — the real code inlines the
choice:

```python
keep_alive=settings.pagination.scroll_keep_alive
           if settings.pagination.strategy == "scroll"
           else settings.pagination.pit_keep_alive
```

A type-along reader references an undefined name. **Fix:** either show the inline expression, or add a
one-line "we factor the keep-alive choice into a tiny `_keep_alive(settings)` helper" and *show that
helper*. (The helper is a fine idea — it's just not in the repo, so the doc and code must agree.)

### T4 (P1) — tickets reference test files the repo does not contain

ETL-A2 ends on `tests/test_models.py` and ETL-A4 on `tests/test_logging.py`. Neither file exists in
`tests/`. This is *why* `logging_setup.py` sits at 0% coverage. As a build log this is defensible, but
the brief asks whether the tutorial covers the code as shipped — and here the milestone commands
(`pytest tests/test_models.py`, `pytest tests/test_logging.py`) would both error with "file not
found." **Fix options:** (a) add the two test files to the repo so the tutorial is literally
runnable, or (b) note in the tutorial that A2/A4 tests are folded into other modules and adjust the
milestone commands. Recommended: (a) — it also closes the 0% coverage gap on `logging_setup.py`.

### T5 (P1) — the `_id` mirage is reproduced in the tutorial

ETL-B1 prints `iter_transformed` with `hit_id = hit.get("_id")` and uses a fixture
`DOC = {…, "_id": "h1"}`. Combined with the README's job-doc example that maps a column to `_id` (see
§4), this teaches that a hit carries `_id` and that you can project it. **You cannot** — by the time
`iter_transformed` runs, pagination has already reduced each hit to its `_source`, so `hit.get("_id")`
is `None` and a `"_id"` path resolves to `""` (proof in §3.1). The tutorial should add one sentence at
ETL-B1: *"Note: by this point each hit is the document's `_source`; the envelope (`_id`, `sort`) is
gone, so a column cannot be mapped to `_id` unless pagination is changed to carry it (see REVIEW §3.1)."*

## 1.2 Coverage — does the tutorial cover *all* of the code?

Mostly yes. The table maps each shipped module to where the tutorial builds it. "Gap" = something in
the real module the tutorial never shows or mentions.

| Module | Ticket | Covered? | Gap to close |
|---|---|---|---|
| `errors.py` | A2 | ✓ | `jolt_op`/JOLT docstring carried over verbatim — see §3.2 |
| `models.py` | A2 | ✓ | — |
| `config.py` | A3 | ✓ (abridged, flagged) | `_get_float`, `SftpConfig.remote_dir` semantics never shown; both are real config surface |
| `logging_setup.py` | A4 | ✓ | test file absent (T4) |
| `retry.py` | A5 | ✓ | `@retry` decorator only mentioned parenthetically; fine |
| `transformer.py` | B1 | ✓ | the `_id` caveat (T5) |
| `csv_writer.py` | B2 | ✓ | `_stringify` of **nested** dict/list values not discussed — see §3.5 |
| `pagination/base.py` | C1 | ✓ | — |
| `pagination/scroll.py` | C1 | ✓ | error handling dropped (T1/T2) |
| `pagination/search_after.py` | C1 | ✓ (fragment, flagged) | `pit_id = resp.get("pit_id", pit_id)` refresh not explained |
| `extractor.py` | C1 | ✓ | — |
| `job_loader.py` | C2 | ✓ | — |
| `validator.py` | D1 | ✓ | — |
| `sftp_uploader.py` | D2 | ✓ | destination atomicity / ordering contract — see §3.3 |
| `control_consumer.py` | E1 | ✓ | — |
| `pipeline.py` | E2 | ✓ | `_keep_alive` mismatch (T3); `initial_count` is computed-but-only-logged (§3.6) |
| `__main__.py` | E3 | ✓ (abridged) | the abridged `main()` omits the `_log.info("starting etl daemon")` line that E3's DoD checks for; `_build_es_client` body never shown |
| `__init__.py` | — | ✗ | `__version__ = "0.1.0"` never mentioned (trivial) |

**Net:** no module is missing, but four real behaviors are absent from the tutorial — ES error
wrapping in scroll (T1/T2), the `_id` limitation (T5), SFTP non-atomic delivery (§3.3), and nested
value stringification (§3.5). The first two are the ones worth fixing.

## 1.3 Smaller nits

- **N1 (P2):** ETL-E3 DoD says the daemon "logs 'starting'", but the abridged `main()` in that ticket
  doesn't include the log line. Add it to the snippet so the DoD is self-consistent.
- **N2 (P2):** Appendix A core-build total ("≈ 12–17 engineer-days") and the per-ticket estimates are
  fine, but there is no link from the tutorial to `DESIGN.md` sections per ticket. A "see DESIGN §N"
  cross-link in each ticket would let the two docs reinforce each other (they currently only reference
  each other once, in the intro).

---

# Part 2 — DESIGN.md: correctness & completeness

DESIGN.md is accurate and thorough; the test-count and coverage figures it cites check out (the deprecation
of `body=` under elasticsearch-py 8.x→9.x, the cumulative-commit reasoning). The defects are mostly
**file corruption** at the top, plus a few **unstated design choices**.

## 2.1 Corruption / rendering defects

### D1 (P0) — the H1 title is corrupted

Line 1 is literally `Pycharm3# Design & Implementation Guide`. The stray `Pycharm3` prefix (an
editor mishap) means the `#` is no longer at line-start, so **the document renders with no H1** — the
title shows as plain text. Fix to `# Design & Implementation Guide`.

### D2 (P0) — stray `33` on line 2

Line 2 is a bare `33` (more stray keystrokes). Delete it.

### D3 (P1) — `safeHiHHHty` typo

Line 80: "the kind of `safeHiHHHty` check that is easy to skip" → `safety`.

> These three are almost certainly the same accidental-keystroke event (note `Pycharm3`). They are
> trivial to fix but currently the *first* thing a reader sees, so they punch above their weight.

## 2.2 Accuracy notes (small)

- **§14 vs README:** DESIGN's job-doc example (§14) correctly uses `"order_id": "order_id"`. The
  README's example uses `"order_id": "_id"`, which is **broken** (§4/§3.1). DESIGN is right; README is
  wrong. Worth a one-line note in DESIGN §22 making the `_id` limitation explicit so nobody "fixes"
  DESIGN to match the README.
- **§20/§26:** "`expected_count` … the *ground truth* the validator will check against … record N up
  front" slightly oversells the first count call: the validator re-queries `_count` itself, so the
  pipeline's initial `expected_count` is **only logged**, never compared (§3.6). Phrase it as "logged
  for observability" to avoid implying it feeds validation.

## 2.3 Completeness — design choices that deserve a paragraph

These are decisions the code makes that DESIGN.md (the "explain every *why*" doc) is silent on:

- **C1 (P1) — SFTP delivery is not atomic at the destination.** §25 covers host-key security
  thoroughly but never addresses *visibility*: the uploader `put`s straight to the final
  `remote_filename`, so a partner polling the drop directory can observe a half-written CSV. The code
  *does* upload the data file before the `.sha256` sidecar, which is a usable "sidecar = ready" signal
  — but that contract is undocumented. DESIGN should either document the ordering as the delivery
  contract, or note the temp-upload-then-rename pattern as the hardening step. (See §3.3.)
- **C2 (P2) — nested values stringify to Python `repr`.** §23 mentions `_stringify` for `None`/`bool`
  but not that a column whose path resolves to an object/array becomes `str(dict)` → `{'a': 1}` (single
  quotes, not JSON). For a flat-CSV tool this is an edge case, but it's a real output behavior worth a
  sentence (§3.5).
- **C3 (P2) — `ConfigError` is an `EtlError` but is handled out-of-band.** §7's table lists
  `ConfigError` under the `EtlError` umbrella "the daemon catches at one boundary," yet `__main__`
  catches `ConfigError` *separately before the loop* and exits 2. That's correct (config failure is
  startup, not job-scoped), but the hierarchy diagram implies uniform handling. One clause — "except
  `ConfigError`, which is fatal at startup and handled before the loop" — removes the ambiguity.

---

# Part 3 — Code blind spots the docs should reflect

These are properties of the code itself. They belong here because fixing the docs without noting them
would just document a latent issue as if intentional.

## 3.1 (P0/P1) The document `_id` is unreachable by column projection

**The chain:** both pagination strategies `yield h.get("_source", {})`, discarding the hit envelope.
`transformer.iter_transformed` then does `hit_id = hit.get("_id")` on that `_source` dict, and
`get_by_path` resolves column paths against it. Therefore:

1. `TransformError.hit_id` is **always `None`** in production — the "carries `hit_id` for diagnosis"
   value advertised in DESIGN §7/§22 and `errors.py` is effectively dead. (It works in unit tests only
   because they pass `hit_id=` explicitly or use a fixture with a literal `_id` key.)
2. A job that maps a column to `"_id"` gets an **empty string**, silently. Proof:

   ```python
   >>> list(iter_transformed([{"order_id": "o-1", "customer": {"name": "Acme"}}],
   ...                        {"order_id": "_id", "cust": "customer.name"},
   ...                        ["order_id", "cust"], job_id="j"))
   [{'order_id': '', 'cust': 'Acme'}]      # order_id is blank — _id is not in _source
   ```

This is a **genuine capability gap**: the ES `_id` is, for many exports, the natural primary key, and
the current pipeline cannot put it in a column. **Options:**

- *Doc-only (cheap):* state the limitation in DESIGN §22, TUTORIAL ETL-B1, and the README — "paths
  resolve against `_source`; the envelope (`_id`, `sort`, `_score`) is not available."
- *Code fix (enables the feature):* have pagination yield a thin merge, e.g.
  `{**h.get("_source", {}), "_id": h.get("_id")}` (or pass the full hit and have the transformer read
  `_source`), so `"_id"` becomes a usable path. If you do this, `transformer`'s `hit.get("_id")` starts
  working too, and the README example becomes correct instead of broken.

Either way the three docs and the code must end up telling the same story; today they don't.

## 3.2 (P2) JOLT leftovers contradict the "we removed JOLT" decision

The project deliberately dropped JOLT (transformer docstring, README "A note on JOLT", DESIGN §21),
but stale JOLT references remain in *code* and *metadata*:

- `errors.py:33-44` — `TransformError`'s docstring says "Raised when applying the **JOLT** spec to an
  ES hit fails … the offending **JOLT** operation name", and it still carries a `jolt_op` parameter
  that nothing sets or reads. The tutorial reproduces the `jolt_op` param (ETL-A2) verbatim.
- `pyproject.toml:8` (and the generated `src/…egg-info/PKG-INFO`) — `description = "… Kafka control
  topic -> Elasticsearch -> **JOLT** -> CSV -> SFTP"`.

**Fix:** rewrite the `TransformError` docstring to describe dotted-path projection, drop the dead
`jolt_op` field (and its mirror in the tutorial), and update the `pyproject` description to "… ->
dotted-path projection -> CSV -> SFTP" (regenerate the egg-info or ignore it).

## 3.3 (P1) SFTP upload has no destination atomicity

`sftp_uploader._build_batch` emits `put <local> <final-remote>` for each file. There is no
upload-to-temp-then-`rename`, so a consumer watching the remote directory can read a partially
transferred CSV. The implicit safety valve is ordering: the CSV is `put` before its `.sha256`, so a
consumer that waits for the sidecar is safe — **but nothing documents that as the contract, and a
single batch doesn't guarantee the sidecar lands last under all `sftp` error modes.** Improvement:
`put` to `remote.tmp`, then `rename` to the final name (atomic on POSIX servers), and/or document the
"wait for the sidecar" contract explicitly. This is the kind of operational detail DESIGN §25 is the
right home for.

## 3.4 (P2) `_HashingWriter.write` returns `len(s)`, not the bytes written

`csv_writer._HashingWriter.write` returns `len(s)` (character count) while it writes `len(b)` UTF-8
bytes. `csv.DictWriter` ignores the return value, so this is harmless today — but it violates the
`io`-style `write` contract (return bytes written) and would mislead any future caller that trusts it.
Return `len(b)`.

## 3.5 (P2) Nested values become Python `repr`, not JSON

Covered in §2.3 C2 — if a `column_paths` entry resolves to a dict/list, the cell is `str(value)`
(`{'k': 'v'}`), not JSON. Fine for the documented flat-export use case; surprising if someone points a
column at an object. Either document it or `json.dumps` non-scalars in `_stringify`.

## 3.6 (P2) The pipeline's initial `_count` is redundant work

`pipeline.run_one` calls `expected_count(...)` once up front (logged as `expected_count=N`), then
`validate_with_retry` issues its *own* fresh `_count` on the first comparison. So every job runs one
extra `_count` whose result is only logged. Harmless and cheap, but if you want to trim it, pass the
initial count into the validator as its first comparison value instead of re-querying. (If you keep
it, see the §2.2 wording fix so DESIGN doesn't imply it's the validation source of truth.)

---

# Part 4 — README & metadata

- **R1 (P1) — broken job-doc example.** README's job document maps `"order_id": "_id"` (and the prose
  "Columns absent from the map fall back to a same-named top-level key in the hit's `_source`"
  reinforces a `_source`-only model while the example contradicts it). Per §3.1 this yields an empty
  column. Change the example to a real `_source` field (the seed script already uses
  `"order_id": "order_id"`), or fix the code per §3.1 and keep the example.
- **R2 (P2) — test count drift.** README line 90 says "63 tests"; the suite collects **64**. DESIGN
  already says 64. Bump the README.
- **R3 (P2) — JOLT in `pyproject` description.** Same as §3.2.

---

# Part 5 — Prioritized punch-list

Smallest-effort-first within each tier; all references are `file:line` or `DOC §`.

**P0 — fix first (broken as written):**

1. DESIGN.md:1 — remove `Pycharm3`, restore `# Design & Implementation Guide` (D1).
2. DESIGN.md:2 — delete stray `33` (D2).
3. TUTORIAL.md ETL-C1 — fix `scroll.py` so it lints clean: restore the `try/except`+guard or drop the
   unused `ElasticsearchQueryError` import (T1).
4. TUTORIAL.md ETL-E2 — replace `_keep_alive(settings)` with the real inline expression (or define the
   helper) (T3).
5. README.md job-doc example — stop mapping a column to `_id`, or implement §3.1 (R1).

**P1 — fix soon (misleading / capability gap):**

6. Decide the `_id` story (doc-only or code) and align README + DESIGN §22 + TUTORIAL ETL-B1 + the
   code (§3.1 / T5).
7. TUTORIAL.md — flag `scroll.py`/`search_after.py` consistently as simplified, or show the real files
   (T2).
8. TUTORIAL.md — add `tests/test_models.py` and `tests/test_logging.py` to the repo, or fix the A2/A4
   milestone commands (T4); this also lifts `logging_setup.py` off 0% coverage.
9. DESIGN.md §25 — document SFTP delivery atomicity / sidecar-ordering contract (C1 / §3.3).
10. DESIGN.md:80 — `safeHiHHHty` → `safety` (D3).

**P2 — polish:**

11. Purge JOLT leftovers: `errors.py:33-44` docstring + `jolt_op`, `pyproject.toml:8`, tutorial ETL-A2
    (§3.2 / R3).
12. README.md:90 — "63 tests" → "64" (R2).
13. DESIGN §2.2/§20/§26 — reword the initial-`_count` "ground truth" claim (§3.6).
14. DESIGN §7 — note `ConfigError` is handled before the loop (C3).
15. `csv_writer._HashingWriter.write` — return `len(b)` (§3.4); document or JSON-encode nested values
    (§3.5).
16. Cross-link TUTORIAL tickets ↔ DESIGN sections (N2); add the `__version__` mention (tutorial) and
    the "starting" log line to the E3 snippet (N1).

---

# Appendix — quick verification commands

```bash
# Test count + coverage (backs the 64 / 83% / 0%-modules claims)
.venv/bin/pytest -q --cov=etl --cov-report=term-missing

# Reproduce the _id blind spot (§3.1)
.venv/bin/python -c "from etl.transformer import iter_transformed; \
print(list(iter_transformed([{'order_id':'o-1','customer':{'name':'Acme'}}], \
{'order_id':'_id','cust':'customer.name'}, ['order_id','cust'], job_id='j')))"
# -> [{'order_id': '', 'cust': 'Acme'}]

# Reproduce the tutorial scroll.py lint failure (T1): paste the ETL-C1 block into a file, then
.venv/bin/ruff check that_file.py    # -> F401 ElasticsearchQueryError imported but unused

# Surface remaining JOLT references (§3.2)
grep -rni jolt src/ pyproject.toml README.md
```
