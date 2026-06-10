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
