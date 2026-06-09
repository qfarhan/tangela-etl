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
