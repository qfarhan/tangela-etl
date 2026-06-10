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
