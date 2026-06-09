"""Pagination strategy protocol and factory for the ETL pipeline.

The :class:`PaginationStrategy` protocol is re-exported from the standalone
:mod:`es_extract` package so both layers speak the same structural type. The
factory builds the ``etl`` wrapper strategies, which pin failures to
:class:`etl.errors.ElasticsearchQueryError`.
"""

from __future__ import annotations

from es_extract.pagination import PaginationStrategy
from etl.errors import ConfigError

__all__ = ["PaginationStrategy", "make_strategy"]


def make_strategy(name: str, *, keep_alive: str) -> PaginationStrategy:
    """Return a strategy instance by short name.

    ``keep_alive`` is passed to the implementation that needs it (scroll TTL
    or PIT keep-alive).
    """
    name = name.lower()
    # Local imports avoid a circular import with the package __init__.
    if name == "scroll":
        from etl.pagination.scroll import ScrollPagination

        return ScrollPagination(keep_alive=keep_alive)
    if name == "search_after":
        from etl.pagination.search_after import SearchAfterPagination

        return SearchAfterPagination(keep_alive=keep_alive)
    raise ConfigError(f"unknown pagination strategy: {name!r}")
