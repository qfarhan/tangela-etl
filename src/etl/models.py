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
