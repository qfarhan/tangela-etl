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
