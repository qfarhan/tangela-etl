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
