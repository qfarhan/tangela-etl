"""Bridges the ES client to a pagination strategy.

The extractor is intentionally thin: it owns the ``_count`` call (delegated to
the standalone :mod:`es_extract` package, with failures pinned to
:class:`etl.errors.ElasticsearchQueryError`) and delegates hit iteration to
whichever :class:`~etl.pagination.base.PaginationStrategy` was selected.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from es_extract.extract import count as _count
from etl.errors import ElasticsearchQueryError
from etl.models import JobSpec
from etl.pagination.base import PaginationStrategy


def expected_count(es: Any, index: str, query: dict[str, Any]) -> int:
    return _count(es, index, query, error_cls=ElasticsearchQueryError)


def iter_hits(
    es: Any,
    job: JobSpec,
    strategy: PaginationStrategy,
    *,
    page_size: int,
) -> Iterator[dict[str, Any]]:
    return strategy.iter_hits(
        es=es,
        index=job.data_index,
        query=job.query,
        page_size=page_size,
    )
