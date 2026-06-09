"""Top-level extraction helpers: ``count`` and a one-call ``iter_hits``.

``iter_hits`` is the convenience entry point for callers who just want "stream
every hit this query matches" without wiring a strategy by hand. Pass a
strategy *name* (it builds one) or a ready-made ``PaginationStrategy``.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from es_extract.errors import EsExtractError
from es_extract.pagination import PaginationStrategy, make_strategy


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
    strategy: str | PaginationStrategy = "scroll",
    page_size: int = 1000, keep_alive: str = "5m", source_only: bool = True,
    error_cls: type[Exception] = EsExtractError,
) -> Iterator[dict[str, Any]]:
    """Stream every hit ``query`` matches, paginating with ``strategy``.

    ``strategy`` may be a name (``"scroll"`` / ``"search_after"``) — in which
    case ``keep_alive`` / ``source_only`` / ``error_cls`` configure the strategy
    that gets built — or an already-constructed ``PaginationStrategy``.
    """
    strat = (
        strategy
        if not isinstance(strategy, str)
        else make_strategy(
            strategy, keep_alive=keep_alive, source_only=source_only, error_cls=error_cls
        )
    )
    return strat.iter_hits(es=es, index=index, query=query, page_size=page_size)
